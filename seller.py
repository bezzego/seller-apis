import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """
    Получает список товаров магазина Озон.

    Аргументы:
        last_id (str): Идентификатор последнего товара для пагинации.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Возвращает:
        dict: Результат с информацией о товарах.

    Примеры:
        >>> get_product_list("", "123", "token")
        {...}

    Исключения:
        requests.exceptions.HTTPError: если API возвращает ошибку
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """
    Получает артикулы товаров магазина Озон.

    Аргументы:
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Возвращает:
        list: Список артикулов товаров.

    Примеры:
        >>> get_offer_ids("123", "token")
        ['offer1', 'offer2', ...]

    Исключения:
        requests.exceptions.HTTPError: если API возвращает ошибку
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """
    Обновляет цены товаров.

    Аргументы:
        prices (list): Список цен для обновления.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Возвращает:
        dict: Ответ API после обновления цен.

    Примеры:
        >>> update_price([{"offer_id": "123", "price": 1000}], "123", "token")
        {...}

    Исключения:
        requests.exceptions.HTTPError: если API возвращает ошибку
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """
    Обновляет остатки товаров.

    Аргументы:
        stocks (list): Список остатков для обновления.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Возвращает:
        dict: Ответ API после обновления остатков.

    Примеры:
        >>> update_stocks([{"offer_id": "123", "stock": 10}], "123", "token")
        {...}

    Исключения:
        requests.exceptions.HTTPError: если API возвращает ошибку
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """
    Скачивает файл с остатками с сайта Casio.

    Аргументы:
        Нет.

    Возвращает:
        list: Список остатков часов в виде словарей.

    Примеры:
        >>> download_stock()
        [{'Код': '123', 'Количество': '5', 'Цена': "5'990.00 руб."}, ...]

    Исключения:
        requests.exceptions.HTTPError: если не удается скачать файл
        zipfile.BadZipFile: если скачанный файл не является zip архивом
        FileNotFoundError: если файл Excel не найден после распаковки
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """
    Создает список остатков для обновления, фильтруя по артикулу.

    Аргументы:
        watch_remnants (list): Список остатков часов.
        offer_ids (list): Список артикулов товаров.

    Возвращает:
        list: Список словарей с артикулами и остатками.

    Примеры:
        >>> create_stocks([{'Код': '123', 'Количество': '5'}], ['123', '456'])
        [{'offer_id': '123', 'stock': 5}, {'offer_id': '456', 'stock': 0}]

    Исключения:
        ValueError: если количество не может быть преобразовано в int
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Создает список цен для обновления, фильтруя по артикулу.

    Аргументы:
        watch_remnants (list): Список остатков часов.
        offer_ids (list): Список артикулов товаров.

    Возвращает:
        list: Список словарей с информацией о ценах.

    Примеры:
        >>> create_prices([{'Код': '123', 'Цена': "5'990.00 руб."}], ['123'])
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '123', 'old_price': '0', 'price': '5990'}]

    Исключения:
        ValueError: если цена не может быть преобразована
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """
    Преобразует цену из формата с символами в числовую строку.

    Аргументы:
        price (str): Цена в формате, например, "5'990.00 руб.".

    Возвращает:
        str: Преобразованная строка цены, например, "5990".

    Примеры:
        >>> price_conversion("5'990.00 руб.")
        '5990'

    Исключения:
        AttributeError: если price не строка или None
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    Разделяет список на части заданного размера.

    Аргументы:
        lst (list): Исходный список.
        n (int): Размер каждой части.

    Возвращает:
        generator: Части списка длиной n.

    Примеры:
        >>> list(divide([1,2,3,4,5], 2))
        [[1, 2], [3, 4], [5]]

    Исключения:
        TypeError: если lst не список или n не int
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """
    Загружает и обновляет цены товаров партиями.

    Аргументы:
        watch_remnants (list): Список остатков часов.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Возвращает:
        list: Список обновленных цен.

    Примеры:
        >>> import asyncio
        >>> asyncio.run(upload_prices(watch_remnants, "123", "token"))
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '123', 'old_price': '0', 'price': '5990'}]

    Исключения:
        requests.exceptions.HTTPError: если API возвращает ошибку
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """
    Загружает и обновляет остатки товаров партиями.

    Аргументы:
        watch_remnants (list): Список остатков часов.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Возвращает:
        tuple: Кортеж из списка не пустых остатков и полного списка остатков.

    Примеры:
        >>> import asyncio
        >>> asyncio.run(upload_stocks(watch_remnants, "123", "token"))
        ([{'offer_id': '123', 'stock': 5}], [{'offer_id': '123', 'stock': 5}, {'offer_id': '456', 'stock': 0}])

    Исключения:
        requests.exceptions.HTTPError: если API возвращает ошибку
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """
    Основная функция для обновления остатков и цен товаров.

    Аргументы:
        Нет.

    Возвращает:
        Нет.

    Примеры:
        >>> main()
        None

    Исключения:
        Exception: общая ошибка при выполнении
    """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
