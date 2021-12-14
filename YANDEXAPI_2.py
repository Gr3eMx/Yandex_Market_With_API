import requests
import json
from bs4 import BeautifulSoup
import pymysql
from datetime import datetime, timedelta

sku_id = ['']
now = datetime.now().date()
dateFrom = str(now - timedelta(days=1))
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; rv:12.0) Gecko/20100101 Firefox/12.0',
    'Accept-Language': 'en-en,ru;q=0.8,en-us;q=0.5,en;q=0.3',
    'Connection': 'keep-close',
    'Authorization': 'OAuth oauth_token="", oauth_client_id=""'}

def count_sells_yesterday(dateFrom,dateTO):
    body = {
      "dateFrom": dateFrom,
      "dateTo": dateTO,
      "orders":
      [],
      "statuses":
      [
        "PROCESSING"
      ]
    }
    jsonBody = json.dumps(body, ensure_ascii=False).encode('utf-8')
    reques = requests.post('https://api.partner.market.yandex.ru/v2/campaigns/21962613/stats/orders.json', jsonBody, headers=headers)
    print(reques.text)
    with open('yandex1_2.json', 'w', encoding='utf-8') as file:
        json.dump(reques.json(), file, ensure_ascii=False, indent=4)
    with open('yandex1_2.json', encoding='utf-8') as file:
        file_content = file.read()
    soup = BeautifulSoup(file_content, 'html.parser')
    site_json = json.loads(soup.text)
    count_sells_sht = {id : 0 for id in sku_id}
    for sku in sku_id:
        for i in site_json['result']['orders']:
                for z in i['items']:
                    if sku in z['shopSku']:
                        count_sells_sht[sku] += z['count']
    return count_sells_sht

def count_sells_rub():
    with open('yandex1_2.json', encoding='utf-8') as file:
        file_content = file.read()
    soup = BeautifulSoup(file_content, 'html.parser')
    site_json = json.loads(soup.text)
    count_sells_sht = {id : 0 for id in sku_id}
    for sku in sku_id:
        for i in site_json['result']['orders']:
                for z in i['items']:
                    for k in z['prices']:
                        if sku in z['shopSku']:
                            count_sells_sht[sku] += k['total']
    return count_sells_sht

def count_canselled_items(dateFrom,dateTO):
    body = {
        "dateFrom": dateFrom,
        "dateTo": dateTO,
        "orders":
            [],
        "statuses":
            [
                "CANCELLED_BEFORE_PROCESSING",
                "CANCELLED_IN_DELIVERY",
                "CANCELLED_IN_PROCESSING"
            ]
    }
    jsonBody = json.dumps(body, ensure_ascii=False).encode('utf-8')
    reques = requests.post('https://api.partner.market.yandex.ru/v2/campaigns/21962613/stats/orders.json', jsonBody, headers=headers)

    with open('yandex_2.json', 'w', encoding='utf-8') as file:
        json.dump(reques.json(), file, ensure_ascii=False, indent=4)
    with open('yandex_2.json', encoding='utf-8') as file:
        file_content = file.read()
    soup = BeautifulSoup(file_content, 'html.parser')
    site_json = json.loads(soup.text)
    count_cansellde = {id : 0 for id in sku_id}
    for sku in sku_id:
        for i in site_json['result']['orders']:
            for z in i['items']:
                if sku in z['shopSku']:
                    count_cansellde[sku] += z['count']
    return count_cansellde

def count_returned(dateFrom,dateTO):
    body = {
        "dateFrom": dateFrom,
        "dateTo": dateTO,
        "orders":
            [],
        "statuses":
            [
                "RETURNED",
            ]
    }
    
    jsonBody = json.dumps(body, ensure_ascii=False).encode('utf-8')
    reques = requests.post('https://api.partner.market.yandex.ru/v2/campaigns/21962613/stats/orders.json', jsonBody, headers=headers)
    with open('yandex2_2.json', 'w', encoding='utf-8') as file:
        json.dump(reques.json(), file, ensure_ascii=False, indent=4)
    with open('yandex2_2.json', encoding='utf-8') as file:
        file_content = file.read()
    soup = BeautifulSoup(file_content, 'html.parser')
    site_json = json.loads(soup.text)
    count_returned = {id : 0 for id in sku_id}
    for sku in sku_id:
        for i in site_json['result']['orders']:
            for z in i['items']:
                if sku in z['shopSku']:
                    count_returned[sku] += z['count']
    return count_returned

def count_sklad():
    body = {
  "shopSkus": sku_id
}
    jsonBody = json.dumps(body, ensure_ascii=False).encode('utf-8')
    reques = requests.post('https://api.partner.market.yandex.ru/v2/campaigns/21962613/stats/skus.json', jsonBody, headers=headers)
    with open('yandex3_2.json', 'w', encoding='utf-8') as file:
        json.dump(reques.json(), file, ensure_ascii=False, indent=4)
    with open('yandex3_2.json', encoding='utf-8') as file:
        file_content = file.read()
    soup = BeautifulSoup(file_content, 'html.parser')
    site_json = json.loads(soup.text)
    count_items_sklad = {}
    for i in site_json['result']['shopSkus']:
        for z in i['warehouses']:
            for k  in z['stocks']:
                if k['type'] == 'FIT':
                    count_items_sklad[i['shopSku']] = k['count']
    for l in sku_id:
        if l not in count_items_sklad:
            count_items_sklad[l] = 0
    return count_items_sklad

def insert_in_DB():
    connection = pymysql.connect()
    with open('data_yesterday_2.json', encoding='utf-8') as file:
        file_content = file.read()
    soup = BeautifulSoup(file_content, 'html.parser')
    site_json = json.loads(soup.text)
    totals_count = 0
    totals_pr_rub = 0
    totals_sklad = 0
    totals_ref = 0
    totals_canc = 0
    print(dateFrom)
    for c in site_json:
        totals_count += float(c['sells_yesterday'])
        totals_pr_rub += float(c['sells_rub'])
        totals_sklad += c['count_sklad']
        totals_ref += c['count_returned']
        totals_canc += c['count_canselled']

    with connection.cursor() as cursor:
        cursor.execute('SELECT other_id, main_id FROM main_yandex2 WHERE is_published = 1')
        id_mains_sku = dict(cursor.fetchall())
    for i in site_json:
        # try:
        #     with connection.cursor() as cursor:
        #         cursor.execute(f"CREATE TABLE yandex2_{id_mains_sku[i['sku_id']]}(ID int NOT NULL AUTO_INCREMENT, date varchar(25) NOT NULL,  pr varchar(25) NOT NULL, pr_rub varchar(25) NOT NULL, stock varchar(25) NOT NULL, refund varchar(25) NOT NULL, cancel varchar(25) NOT NULL, conversion varchar(25) NOT NULL, PRIMARY KEY (ID))")
        # except Exception as ex:
        #         print('Не получилось создать')
        #         print(ex)
        try:
            with connection.cursor() as cursor:
                cursor.execute(f'SELECT date,pr FROM yandex2_{id_mains_sku[i["sku_id"]]}')
                date_last = dict(cursor.fetchall())
            if dateFrom not in date_last:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"INSERT INTO yandex2_{id_mains_sku[i['sku_id']]} (date,pr,pr_rub,stock,refund,cancel,conversion) VALUES ('{dateFrom}', {i['sells_yesterday']}, {i['sells_rub']}, {i['count_sklad']}, {i['count_returned']}, {i['count_canselled']}, 0)")
            else:
                continue
        except Exception as ex:
            print('Не получилось добавить')
            print(ex)
    # try:
    #     with connection.cursor() as cursor:
    #         cursor.execute(f"CREATE TABLE yandex2_totals (ID int NOT NULL AUTO_INCREMENT, date varchar(25) NOT NULL,  pr varchar(25) NOT NULL, pr_rub varchar(25) NOT NULL, stock varchar(25) NOT NULL, refund varchar(25) NOT NULL, cancel varchar(25) NOT NULL, conversion varchar(25) NOT NULL, turn_enemy varchar(25) NOT NULL, PRIMARY KEY (ID))")
    # except Exception as ex:
    #     print('Не создал таблицу')
        print(ex)
    try:
        with connection.cursor() as cursor:
            cursor.execute(f'SELECT date,pr FROM yandex2_totals')
            date_last1 = dict(cursor.fetchall())
        if dateFrom not in date_last1:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"INSERT INTO yandex2_totals (date,pr,pr_rub,stock,refund,cancel,conversion,turn_enemy) VALUES ('{dateFrom}', {totals_count}, {totals_pr_rub}, {totals_sklad}, {totals_ref}, {totals_canc}, 0, 0 )")
        connection.commit()
    except Exception as ex:
        print('Не получилось засунуть данные в таблицу')
        print(ex)
    finally:
        connection.close()

def main():
    jso = []
    for sku in sku_id:
        jso.append({
            'sku_id': sku,
            'sells_yesterday': count_sells_yesterday(dateFrom, dateFrom)[sku],
            'sells_rub': count_sells_rub()[sku],
            'count_canselled': count_canselled_items(dateFrom, dateFrom)[sku],
            'count_returned': count_returned(dateFrom, dateFrom)[sku],
            'count_sklad': count_sklad()[sku]
        })
    with open('data_yesterday_2.json', 'w', encoding='utf-8') as file:
        json.dump(jso, file, ensure_ascii=False ,indent=False)
    insert_in_DB()

if __name__ == "__main__":
    main()
