import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получает список товаров из API Яндекс.Маркета.

    Аргументы:
        page (str): Токен страницы.
        campaign_id (str): Идентификатор рекламной кампании.
        access_token (str): Токен доступа для авторизации.

    Возвращаемое значение:
        list: Список товаров, полученных из ответа API.

    Исключения:
        requests.exceptions.RequestException: Если возникает ошибка при
        отправке запроса.
        ValueError: Если 'result' отсутствует в ответе API.
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
    """Обновляет информацию о наличии товаров на Яндекс.Маркете.

    Аргументы:
        stocks (list): Список словарей с данными о товарах и их наличии.
        campaign_id (str): Идентификатор рекламной кампании.
        access_token (str): Токен доступа для авторизации.

    Возвращаемое значение:
        dict: Ответ API в формате JSON.

    Исключения:
        requests.exceptions.RequestException: Если возникает ошибка
        при отправке запроса.
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
    """Обновляет цены предложений в рекламной кампании на Яндекс.Маркете.

    Аргументы:
        prices (list): Список словарей с данными о предложениях
        и их новых ценах.
        campaign_id (str): Идентификатор рекламной кампании.
        access_token (str): Токен доступа для авторизации.

    Возвращаемое значение:
        dict: Ответ API в формате JSON.

    Исключения:
        requests.exceptions.RequestException: Если возникает ошибка
        при отправке запроса.
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
    """Извлекает артикулы товаров из каталога Яндекс.Маркета.

    Аргументы:
        campaign_id (str): Идентификатор рекламной кампании.
        market_token (str): Токен доступа для авторизации.

    Возвращаемое значение:
        list: Список артикулов товаров.

    Исключения:
        requests.exceptions.RequestException: Если возникает ошибка
        при отправке запроса.
        ValueError: Если 'offerMappingEntries' или 'paging' отсутствуют
        в ответе API.
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
    """Создаёт список данных о наличии товаров для обновления на маркетплейсе.

    Аргументы:
        watch_remnants (list): Список данных о товарах, содержащий их
            артикулы и количество.
        offer_ids (list): Список артикулов товаров, которые необходимо
            обработать.
        warehouse_id (str): Идентификатор склада, для которого необходимо
            создать записи о товарах.

    Возвращаемое значение:
        list: Список словарей с данными о товарах и их количестве,
            готовых для отправки в систему.

    Пример использования:
        watch_remnants = [
            {"Код": "123", "Количество": "10"},
            {"Код": "456", "Количество": ">10"},
        ]
        offer_ids = ["123", "456", "789"]
        warehouse_id = "warehouse_1"
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
        print(stocks)

    Исключения:
        Нет явных исключений, но могут возникнуть ошибки, связанные
        с типами данных
        или переданными значениями.
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(
        microsecond=0).isoformat() + "Z")
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
    """Создаёт список ценовых данных для товаров.

    Аргументы:
        watch_remnants (list): Список словарей с данными о товарах,
            включая их артикулы и цены.
        offer_ids (list): Список артикулов товаров, для которых необходимо
            создать ценовые данные.

    Возвращаемое значение:
        list: Список словарей с ценовой информацией для каждого товара.

    Пример использования:
        watch_remnants = [
            {"Код": "123", "Цена": "1000"},
            {"Код": "456", "Цена": "1500"},
        ]
        offer_ids = ["123", "456"]
        prices = create_prices(watch_remnants, offer_ids)
        print(prices)
        # Вывод: [{'id': '123', 'price': {'value': 1000, 'currencyId': 'RUR'}},
        #         {'id': '456', 'price': {'value': 1500, 'currencyId': 'RUR'}}]

    Исключения:
        ValueError: Если цена товара не может быть преобразована в целое число.
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
    """Обновляет цены товаров на маркетплейсе.

    Аргументы:
        watch_remnants (list): Список словарей с данными о товарах, включая их
            артикулы и цены.
        campaign_id (str): Идентификатор рекламной кампании на маркетплейсе.
        market_token (str): Токен доступа к API маркетплейса.

    Возвращаемое значение:
        list: Список словарей с ценовыми данными для каждого товара.

    Пример использования:
        watch_remnants = [
            {"Код": "123", "Цена": "1000"},
            {"Код": "456", "Цена": "1500"},
        ]
        campaign_id = "campaign_1"
        market_token = "your_market_token"
        prices = await upload_prices(watch_remnants, campaign_id, market_token)
        print(prices)
        # Вывод: [{'id': '123', 'price': {'value': 1000, 'currencyId': 'RUR'}},
        #         {'id': '456', 'price': {'value': 1500, 'currencyId': 'RUR'}}]

    Исключения:
        ValueError: Если цена товара не может быть преобразована в целое число.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Обновляет остатки товаров на маркетплейсе.

    Аргументы:
        watch_remnants (list): Список словарей с данными о товарах, включая их
            артикулы и остатки.
        campaign_id (str): Идентификатор рекламной кампании на маркетплейсе.
        market_token (str): Токен доступа к API маркетплейса.
        warehouse_id (str): Идентификатор склада.

    Возвращаемое значение:
        tuple: Кортеж из двух элементов:
            - list: Список словарей с данными об остатках товаров,
            где количество товара не равно нулю.
            - list: Список всех словарей с данными об остатках товаров.

    Исключения:
        ValueError: Если количество товара не может быть преобразовано
        в целое число.
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
    """Основная функция для обновления остатков и цен.

    FBS (Fulfillment by Seller) — модель, при которой продавец самостоятельно
    управляет складом и логистикой, а Яндекс.Маркет предоставляет только
    платформу для продаж.

    DBS (Delivery by Seller) — модель, при которой продавец отвечает
    за доставку товара покупателю, но использует логистические возможности
    Яндекс.Маркета.

    SKU (Stock Keeping Unit) — уникальный идентификатор товара, используемый
    для учёта и отслеживания остатков на складе.

    Исключения:
        requests.exceptions.ReadTimeout: Если запрос превышает допустимое
        время ожидания.
        requests.exceptions.ConnectionError: Если возникает ошибка сетевого
        подключения.
        Exception: Для любых других исключений.

    Пример корректного использования:
        main()
        # Функция выполнится успешно, если переменные окружения настроены
        # правильно, и запросы на обновление остатков и цен пройдут без ошибок.

    Пример некорректного использования:
        main()
        # Если переменные окружения не настроены или возникает сетевая ошибка,
        # будут возбуждены исключения ReadTimeout или ConnectionError,
        # которые будут выведены на экран.
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
