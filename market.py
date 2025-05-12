import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """
    Получает список товаров с Яндекс Маркета для указанной страницы.

    Аргументы:
        page (str): Токен страницы для пагинации.
        campaign_id (str): Идентификатор кампании.
        access_token (str): OAuth токен для доступа к API.

    Возвращает:
        dict: Результат ответа API с товарами.

    Примеры:
        >>> get_product_list('', '123456', 'abcdef')
        {'offerMappingEntries': [...], 'paging': {...}}

    Исключения:
        requests.exceptions.HTTPError: Если ответ API содержит ошибку.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """
    Обновляет остатки товаров на складе в Яндекс Маркете.

    Аргументы:
        stocks (list): Список остатков товаров.
        campaign_id (str): Идентификатор кампании.
        access_token (str): OAuth токен для доступа к API.

    Возвращает:
        dict: Ответ API после обновления остатков.

    Примеры:
        >>> update_stocks([{"sku": "123", "warehouseId": "1", "items": [{"count": 10, "type": "FIT", "updatedAt": "2023-01-01T00:00:00Z"}]}], '123456', 'abcdef')
        {'result': {...}}

    Исключения:
        requests.exceptions.HTTPError: Если ответ API содержит ошибку.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """
    Обновляет цены товаров в Яндекс Маркете.

    Аргументы:
        prices (list): Список цен на товары.
        campaign_id (str): Идентификатор кампании.
        access_token (str): OAuth токен для доступа к API.

    Возвращает:
        dict: Ответ API после обновления цен.

    Примеры:
        >>> update_price([{"id": "123", "price": {"value": 1000, "currencyId": "RUR"}}], '123456', 'abcdef')
        {'result': {...}}

    Исключения:
        requests.exceptions.HTTPError: Если ответ API содержит ошибку.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """
    Получает список артикулов (shopSku) всех товаров в кампании Яндекс Маркета.

    Аргументы:
        campaign_id (str): Идентификатор кампании.
        market_token (str): OAuth токен для доступа к API.

    Возвращает:
        list: Список артикулов товаров (shopSku).

    Примеры:
        >>> get_offer_ids('123456', 'abcdef')
        ['111', '222', '333']

    Исключения:
        requests.exceptions.HTTPError: Если ответ API содержит ошибку.
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """
    Формирует список остатков для загрузки на Яндекс Маркет.

    Аргументы:
        watch_remnants (list): Список остатков по товарам.
        offer_ids (list): Список артикулов (shopSku), которые уже есть на маркете.
        warehouse_id (str): Идентификатор склада.

    Возвращает:
        list: Список остатков для API Яндекс Маркета.

    Примеры:
        >>> create_stocks([{"Код": "123", "Количество": "5"}], ["123"], "1")
        [{'sku': '123', 'warehouseId': '1', 'items': [{'count': 5, 'type': 'FIT', 'updatedAt': '...'}]}]
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Формирует список цен для загрузки на Яндекс Маркет.

    Аргументы:
        watch_remnants (list): Список товаров с ценами.
        offer_ids (list): Список артикулов (shopSku), которые уже есть на маркете.

    Возвращает:
        list: Список цен для API Яндекс Маркета.

    Примеры:
        >>> create_prices([{"Код": "123", "Цена": "1000"}], ["123"])
        [{'id': '123', 'price': {'value': 1000, 'currencyId': 'RUR'}}]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """
    Загружает цены на Яндекс Маркет пакетами.

    Аргументы:
        watch_remnants (list): Список товаров с ценами.
        campaign_id (str): Идентификатор кампании.
        market_token (str): OAuth токен для доступа к API.

    Возвращает:
        list: Список загруженных цен.

    Примеры:
        >>> await upload_prices([{"Код": "123", "Цена": "1000"}], '123456', 'abcdef')
        [{'id': '123', 'price': {'value': 1000, 'currencyId': 'RUR'}}]

    Исключения:
        requests.exceptions.HTTPError: Если ответ API содержит ошибку.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """
    Загружает остатки товаров на Яндекс Маркет пакетами.

    Аргументы:
        watch_remnants (list): Список остатков по товарам.
        campaign_id (str): Идентификатор кампании.
        market_token (str): OAuth токен для доступа к API.
        warehouse_id (str): Идентификатор склада.

    Возвращает:
        tuple: Список товаров с ненулевыми остатками и полный список остатков.

    Примеры:
        >>> await upload_stocks([{"Код": "123", "Количество": "5"}], '123456', 'abcdef', '1')
        ([{'sku': '123', ...}], [{'sku': '123', ...}])

    Исключения:
        requests.exceptions.HTTPError: Если ответ API содержит ошибку.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """
    Основная функция для обновления остатков и цен на Яндекс Маркете для FBS и DBS кампаний.

    Аргументы:
        Нет.

    Возвращает:
        None

    Примеры:
        >>> main()

    Исключения:
        requests.exceptions.ReadTimeout: Если превышено время ожидания ответа от API.
        requests.exceptions.ConnectionError: Если не удалось соединиться с API.
        Exception: Любые другие ошибки при выполнении.
    """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
