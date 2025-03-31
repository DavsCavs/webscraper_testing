import sqlite3
import requests
from bs4 import BeautifulSoup

# URL of the website to scrape
URL = "https://www.ss.com/lv/transport/cars/bmw/730/page1.html"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}

# Initialize SQLite database
conn = sqlite3.connect("sscom_ads.db")
cursor = conn.cursor()

# Create table
cursor.execute('''
CREATE TABLE IF NOT EXISTS ads (
id INTEGER PRIMARY KEY AUTOINCREMENT,
title TEXT,
year TEXT,
engine_size TEXT,
mileage TEXT,
price TEXT
);
''')


def get_max_page():
    response = requests.get(URL, headers=HEADERS)
    if response.status_code != 200:
        print(f"Failed to fetch page, status code: {response.status_code}")
        exit()
    soup = BeautifulSoup(response.text, "html.parser")
    max_page = soup.select('.navi')[1].text.strip()
    return max_page


def get_soup(URL):
    response = requests.get(URL, headers=HEADERS)
    if response.status_code != 200:
        print(f"Failed to fetch page, status code: {response.status_code}")
        exit()
    soup = BeautifulSoup(response.text, "html.parser")
    return soup

def scrape_insert(soup):
    rows = soup.select("table[align='center'] tr") # Adjusting to correct table structure
    if not rows:
        print("No ads found. Check HTML structure.")
        exit()

    for row in rows:
        columns = row.find_all("td")
        if len(columns) < 7:
            continue # Skip rows that don't have enough columns

        title = columns[2].text.strip() if columns[2] else "Unknown"
        year = columns[3].text.strip() if columns[3] else "Unknown"
        engine_size = columns[4].text.strip() if columns[4] else "Unknown"
        mileage = columns[5].text.strip() if columns[5] else "Unknown"
        price = columns[6].text.strip() if columns[6] else "Unknown"

        cursor.execute("INSERT INTO ads (title, year, engine_size, mileage, price) VALUES (?, ?, ?, ?, ?)",
            (title, year, engine_size, mileage, price))

max_page = get_max_page()
for i in range (0, int(max_page)):
    soup = get_soup(URL)
    scrape_insert(soup)
    URL = URL[:-6] + str(i + 2) + URL[-5:]

conn.commit()
cursor.execute("SELECT COUNT(*) FROM ads")
record_count = cursor.fetchone()[0]
print(f"Inserted {record_count} records into the database.")

conn.close()
