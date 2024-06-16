import sqlite3
import time
import requests
from datetime import date, datetime, timedelta

def get_exchange_rates():
    request = requests.get("https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?json")

    if request.status_code == 200:
        return request.json()
    else:
        print("Failed to fetch data from API - ", request.status_code)
        return None


def create_table(table_name):
    db = sqlite3.connect("rates.db")
    cursor = db.cursor()

    cursor.execute(f'''CREATE TABLE IF NOT EXISTS '{table_name}'
                    (r030 INTEGER,
                    txt TEXT,
                    rate REAL,
                    exchangedate DATE)
                   ''')
    db.commit()
    db.close()

def insert_data(table_name, data):
    db = sqlite3.connect("rates.db")
    cursor = db.cursor()

    cursor.execute(f"INSERT INTO '{table_name}' VALUES (?, ?, ?, ?)",
                   (data['r030'], data['txt'], data['rate'], data['exchangedate']))

    db.commit()
    db.close()

while True:
    rates = get_exchange_rates()

    if rates:
        table_name = date.today().strftime("%Y_m_%d")
        create_table(table_name)

        for item in rates:
            insert_data(table_name, item)

        print("DATA inserted succesfully")

        time_now = datetime.now()
        tomorrow = time_now + timedelta(days = 1)
        tomorrow_midnight = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 0, 0)

        sleep_time = (tomorrow_midnight - time_now).seconds
        time.sleep(sleep_time)

    else:
        print("Failed to fetch data, retiyring in 1 minute...")
        time.sleep(60)