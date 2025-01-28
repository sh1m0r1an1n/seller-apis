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
    """Получает список товаров из магазина Ozon.

    Аргументы:
        last_id (int): Идентификатор последнего полученного товара.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Возвращаемое значение:
        list: Список товаров, доступных в магазине Ozon.

    Пример использования:
        >>> get_product_list(client_id, seller_token)
        [{'id': 1, 'name': 'Товар 1'}, {'id': 2, 'name': 'Товар 2'}, ...]

    Исключения:
        requests.exceptions.RequestException: Если возникает ошибка
        при отправке запроса.
        ValueError: Если 'result' отсутствует в ответе API.
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
    """Получает список артикулов товаров из магазина Ozon.

    Аргументы:
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Возвращаемое значение:
        list: Список артикулов товаров, доступных в магазине Ozon.

    Пример использования:
        >>> get_offer_ids(client_id, seller_token)
        ['offer_id_1', 'offer_id_2', 'offer_id_3']

    Исключения:
        requests.exceptions.RequestException: Если возникает ошибка при
        отправке запроса к API Ozon.
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
    """Обновляет цены товаров на Ozon.

    Аргументы:
        prices (list): Список словарей, каждый из которых содержит
        'offer_id' (str) и 'price' (float).

        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Возвращаемое значение:
        dict: Ответ от API Ozon в формате JSON.

    Исключения:
        requests.exceptions.RequestException: Если возникает ошибка
        при отправке запроса.
        ValueError: Если 'offer_id' или 'price' отсутствуют в одном
        из элементов списка 'prices'.
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
    """Обновляет информацию об остатках товаров в магазине Ozon.

    Аргументы:
        stocks (list): Список словарей с информацией об остатках товаров.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Возвращаемое значение:
        bool: True, если обновление прошло успешно; False в противном случае.

    Пример использования:
        >>> update_stocks(stocks, client_id, seller_token)
        True

    Исключения:
        requests.exceptions.RequestException: Если возникает ошибка при
        отправке запроса к API Ozon.
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
    """Скачивает файл с остатками товаров с указанного URL и извлекает данные.

    Возвращаемое значение:
        list: Список остатков товаров, извлечённых из Excel-файла.

    Пример использования:
        >>> download_stock()
        [{'offer_id': 'offer_id_1', 'stock': 100},
        {'offer_id': 'offer_id_2', 'stock': 50}, ...]

    Исключения:
        requests.exceptions.RequestException: Если возникает ошибка при
        скачивании файла.
        zipfile.BadZipFile: Если скачанный файл не является допустимым
        ZIP-архивом.
        KeyError: Если в данных отсутствуют ожидаемые ключи.
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
    """Формирует список остатков товаров на складе.

    Аргументы:
        watch_remnants (list): Список данных об остатках товаров.
        offer_ids (list): Список идентификаторов предложений.

    Возвращаемое значение:
        list: Список словарей с идентификаторами предложений
        и соответствующими остатками.

    Пример использования:
        create_stocks(watch_remnants, offer_ids)
        # Вывод: [{'offer_id': 'offer_id_1', 'stock': 100},
        #          {'offer_id': 'offer_id_2', 'stock': 50},
        #          {'offer_id': 'offer_id_3', 'stock': 0}]

    Исключения:
        KeyError: Если в данных об остатках отсутствует ключ 'offer_id'
        или 'stock'.
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
    """Формирует список цен на товары.

    Аргументы:
        watch_remnants (list): Список данных об остатках товаров.
        offer_ids (list): Список идентификаторов предложений.

    Возвращаемое значение:
        list: Список словарей с идентификаторами предложений и
        соответствующими ценами.

    Пример использования:
        create_prices(watch_remnants, offer_ids)
        # Вывод: [{'offer_id': 'offer_id_1', 'price': 100},
        #          {'offer_id': 'offer_id_2', 'price': 50},
        #          {'offer_id': 'offer_id_3', 'price': 0}]

    Исключения:
        KeyError: Если в данных об остатках отсутствует ключ 'offer_id'
        или 'stock'.
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
    """Преобразует строковое представление цены в числовой формат.

    Аргументы:
        price (str): Строка, представляющая цену, например, "5'990.00 руб.".

    Возвращаемое значение:
        str: Строка, представляющая числовое значение цены без разделителей,
             например, "5990".

    Пример использования:
        >>> price_conversion("5'990.00 руб.")
        '5990'

    Исключения:
        ValueError: Если строка не может быть преобразована в числовой формат.
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделяет список на части по n элементов.

    Аргументы:
        lst (list): Исходный список, который требуется разделить.
        n (int): Максимальное количество элементов в каждом подсписке.

    Возвращаемое значение:
        generator: Генератор, который поочередно выдаёт подсписки
        исходного списка.

    Пример использования:
        >>> lst = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        >>> n = 3
        >>> for part in divide(lst, n):
        >>>     print(part)
        [1, 2, 3]
        [4, 5, 6]
        [7, 8, 9]

    Исключения:
        ValueError: Если n меньше или равно нулю.
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Асинхронно загружает цены на товары в систему.

    Аргументы:
        watch_remnants (list): Список данных об остатках товаров.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Возвращаемое значение:
        list: Список обновленных цен на товары.

    Пример использования:
        >>> upload_prices(watch_remnants, client_id, seller_token)
        [{'offer_id': 'offer_id_1', 'price': 100},
         {'offer_id': 'offer_id_2', 'price': 50}]

    Исключения:
        ValueError: Если watch_remnants пуст или содержит некорректные данные.
        KeyError: Если в данных отсутствуют необходимые ключи.
        Exception: Если возникает ошибка при обновлении цен.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Асинхронно обновляет остатки товаров в системе.

    Аргументы:
        watch_remnants (list): Список данных об остатках товаров.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Возвращаемое значение:
        tuple: Кортеж из двух списков:
            - not_empty (list): Список остатков товаров, где количество
            больше нуля.
            - stocks (list): Список всех остатков товаров.

    Пример использования:
        >>> not_empty, stocks = await upload_stocks(watch_remnants,
        client_id, seller_token)
        >>> print(not_empty)
        [{'offer_id': 'offer_id_1', 'stock': 100},
        {'offer_id': 'offer_id_2', 'stock': 50}]
        >>> print(stocks)
        [{'offer_id': 'offer_id_1', 'stock': 100},
        {'offer_id': 'offer_id_2', 'stock': 50}]

    Исключения:
        ValueError: Если watch_remnants пуст или содержит некорректные данные.
        KeyError: Если в данных отсутствуют необходимые ключи.
        Exception: Если возникает ошибка при обновлении остатков.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Основная функция программы.

    Исключения:
        ValueError: Если входные данные некорректны.
        FileNotFoundError: Если указанный файл не найден.
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
