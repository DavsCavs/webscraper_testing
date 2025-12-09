import sqlite3
import requests
from bs4 import BeautifulSoup
# import mysql.connector
import time
import random
import re


# konfiguracija

BASE_URL = "https://www.ss.com/lv/transport/cars/bmw/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

brand = None

# savienosanas ar mysql (Laravel db)
# conn = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="password",     
#     database="sscarsdb"      
# )
# cursor = conn.cursor()

# Initialize SQLite database
conn = sqlite3.connect("sscom_ads.db")
cursor = conn.cursor()

# Create table
cursor.execute('''
CREATE TABLE IF NOT EXISTS ads (
id INTEGER PRIMARY KEY AUTOINCREMENT,
title TEXT,
brand TEXT,
model TEXT,
year TEXT,
engine_size TEXT,
mileage TEXT,
price TEXT,
url TEXT
);
''')

# funkcijas

def scrape_page(url):
    """Scrapo vienu lapu un ievieto visus atrastos datus DB"""
    print(f"Scrapo: {url}")

    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        print(f"Kļūda: {response.status_code}")
        return 0, False
    
    if response.url != url:
        return 0, False
    

    soup = BeautifulSoup(response.text, "html.parser")
    pattern = re.compile(r"^tr_\d{8}$")
    rows = soup.find_all("tr", id=pattern)
    if not rows:
        print("Nav vairāk lapu, cikls apstājas.")
        return 0, False

    added = 0
    for row in rows:
        columns = row.find_all("td")
        if len(columns) < 7:
            continue

        title = columns[2].text.strip() if len(columns) > 2 else ""
        year = columns[4].text.strip() if len(columns) > 3 else ""
        engine_size = columns[5].text.strip() if len(columns) > 4 else ""
        mileage = columns[6].text.strip() if len(columns) > 5 else ""
        price = columns[7].text.strip() if len(columns) > 6 else ""
        url_full = "https://www.ss.com" + columns[2].a["href"] if columns[2].a else ""
        model = columns[3].text.strip() if len(columns) > 3 else ""

        # marka un modelis tiek atdalis no markas
        # parts = title.split(" ", 1)
        # brand = parts[0] if len(parts) > 0 else "Nezināms"
        # model = parts[1] if len(parts) > 1 else ""

        try:
            cursor.execute("""
                INSERT INTO ads (title, brand, model, year, engine_size, mileage, price, url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (title, brand, model, year, engine_size, mileage, price, url_full))
            conn.commit()
            added += cursor.rowcount
        except Exception as e:
            print("DB kļūda:", e)
            conn.rollback()

    print(f"Ievietoti {added} ieraksti no lapas.")
    return added, True


# galvena cikla logika

def main():
    global brand
    print("Sākam SS.com datu vākšanu...")
    total_added = 0
    page = 1

    brandParts = BASE_URL.split('/')
    brand = brandParts[6]
                

    while True:
        url = BASE_URL if page == 1 else f"{BASE_URL}page{page}.html"
        added, has_more = scrape_page(url)
        total_added += added

        if not has_more:
            break

        page += 1
        time.sleep(random.randint(1, 2))  # pauze pret parslodzi

    print(f"Gatavs. Kopā pievienoti {total_added} ieraksti.")
    cursor.close()
    conn.close()


# START

if __name__ == "__main__":
    main()
