import mysql.connector
import requests
from bs4 import BeautifulSoup
import time
import random
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

MAIN_URL = "https://www.ss.com/lv/transport/cars/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

EXCLUDE_SLUGS = {"sell", "spare-parts", "oldtimers", "rarities", "wanted", "car-exchange"}
MAX_WORKERS = 5

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "sscarsdb"
}


def get_db():
    return mysql.connector.connect(**DB_CONFIG)


def clean_int(value):
    digits = re.sub(r"\D", "", value)
    return int(digits) if digits else None


def get_image_url(ad_url):
    try:
        time.sleep(random.uniform(0.3, 0.7))
        response = requests.get(ad_url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            return None
        soup = BeautifulSoup(response.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "i.ss.com/gallery" in href and href.endswith(".800.jpg"):
                return href
    except Exception:
        pass
    return None


def get_brand_urls():
    print("Lasu marku sarakstu...")
    response = requests.get(MAIN_URL, headers=HEADERS)
    if response.status_code != 200:
        print(f"Kļūda ielādējot galveno lapu: {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    seen = set()
    brand_urls = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        match = re.match(r"^/lv/transport/cars/([^/]+)/$", href)
        if match:
            slug = match.group(1)
            if slug not in EXCLUDE_SLUGS and slug not in seen:
                seen.add(slug)
                brand_urls.append(("https://www.ss.com" + href, slug))

    print(f"Atrasts {len(brand_urls)} marku.")
    return brand_urls


def scrape_page(url, brand, conn):
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
        return 0, False

    cursor = conn.cursor()
    added = 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for row in rows:
        columns = row.find_all("td")
        if len(columns) < 7:
            continue

        model = columns[3].text.strip() if len(columns) > 3 else ""
        year = columns[4].text.strip() if len(columns) > 3 else ""
        engine_size = columns[5].text.strip() if len(columns) > 4 else ""
        mileage = clean_int(columns[6].text.strip()) if len(columns) > 5 else None
        price = clean_int(columns[7].text.strip()) if len(columns) > 7 else None
        url_full = "https://www.ss.com" + columns[2].a["href"] if columns[2].a else ""

        if not url_full:
            continue

        image_url = get_image_url(url_full)

        try:
            cursor.execute("""
                INSERT IGNORE INTO cars (brand, model, year, engine_size, mileage, price, url, image_url, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (brand, model, year, engine_size, mileage, price, url_full, image_url, now, now))
            conn.commit()
            added += cursor.rowcount
        except Exception as e:
            print(f"DB kļūda ({brand}): {e}")
            conn.rollback()

    cursor.close()
    print(f"[{brand}] Ievietoti {added} ieraksti no {url}")
    return added, True


def scrape_brand(base_url, brand):
    print(f"\n=== Sāku marku: {brand} ===")
    total_added = 0
    page = 1

    conn = get_db()
    try:
        while True:
            url = base_url if page == 1 else f"{base_url}page{page}.html"
            added, has_more = scrape_page(url, brand, conn)
            total_added += added

            if not has_more:
                break

            page += 1
            time.sleep(random.uniform(0.5, 1.5))
    finally:
        conn.close()

    print(f"=== Marka '{brand}' pabeigta. Pievienoti {total_added} ieraksti. ===")
    return total_added


def main():
    print("Sākam SS.com datu vākšanu...")

    brand_urls = get_brand_urls()
    if not brand_urls:
        print("Nav atrasta neviena marka. Pārtraucam.")
        return

    total_added = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_brand, url, brand): brand for url, brand in brand_urls}
        for future in as_completed(futures):
            brand = futures[future]
            try:
                total_added += future.result()
            except Exception as e:
                print(f"Kļūda ({brand}): {e}")

    print(f"\nGatavs. Kopā pievienoti {total_added} ieraksti.")


if __name__ == "__main__":
    main()
