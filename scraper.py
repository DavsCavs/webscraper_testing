import mysql.connector
import requests
from bs4 import BeautifulSoup
import time
import random
import re
from datetime import datetime

MAIN_URL = "https://www.ss.com/lv/transport/cars/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# autogidas.lt requires full browser headers (no brotli) to avoid 403
HEADERS_LT = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

EXCLUDE_SLUGS = {"sell", "spare-parts", "oldtimers", "rarities", "wanted", "car-exchange"}
CATEGORY_SLUGS = {"electric-cars", "sport-cars", "tuned-cars", "exclusive-cars", "retro-cars"}

# Brands whose names are two words — needed to split autoportaal.ee "<h2>Brand Model</h2>"
MULTI_WORD_BRANDS = {
    "Alfa Romeo", "Aston Martin", "Land Rover", "Range Rover",
    "Rolls Royce", "Rolls-Royce", "Mercedes Benz", "Mercedes-Benz",
}

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "sscarsdb"
}


def get_db():
    # Opens and returns a new MySQL connection.
    return mysql.connector.connect(**DB_CONFIG)


def clean_int(value):
    # Strips all non-digit characters and returns an integer, or None if empty.
    digits = re.sub(r"\D", "", value)
    return int(digits) if digits else None


def clean_mileage(value):
    # Parses Latvian mileage strings; multiplies by 1000 if "tūkst" suffix is present.
    v = value.lower().strip()
    digits = re.sub(r"\D", "", v)
    if not digits:
        return None
    num = int(digits)
    if "tūkst" in v or "tūk" in v:
        num *= 1000
    return num


def get_image_url(ad_url):
    # Visits an ss.com ad page and returns the first gallery image URL (.800.jpg).
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
    # Fetches the ss.com cars main page and returns a list of (url, slug, slug) for each brand.
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
                brand_urls.append(("https://www.ss.com" + href, slug, slug))

    print(f"Atrasts {len(brand_urls)} marku.")
    return brand_urls


def scrape_page(url, brand, slug, conn):
    # Scrapes one ss.com listing page and inserts all found cars into the DB.
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

        # Category pages (electric-cars, sport-cars, tuned-cars, etc.) have an
        # extra brand column at index 3, shifting all other fields to the right.
        if slug in CATEGORY_SLUGS or len(columns) >= 9:
            brand_links = columns[3].find_all("a")
            actual_brand = brand_links[-1].text.strip() if brand_links else columns[3].text.strip()
            model_links = columns[4].find_all("a")
            model = model_links[-1].text.strip() if model_links else columns[4].text.strip()
            year = columns[5].text.strip()
            engine_size = columns[6].text.strip()
            mileage = clean_mileage(columns[7].text.strip())
            price = clean_int(columns[8].text.strip())
        elif len(columns) >= 8:
            actual_brand = brand
            model_links = columns[3].find_all("a")
            model = model_links[-1].text.strip() if model_links else columns[3].text.strip()
            year = columns[4].text.strip()
            engine_size = columns[5].text.strip()
            mileage = clean_mileage(columns[6].text.strip())
            price = clean_int(columns[7].text.strip())
        else:
            continue

        url_full = "https://www.ss.com" + columns[2].a["href"] if columns[2].a else ""

        if not url_full:
            continue

        image_url = get_image_url(url_full)

        try:
            cursor.execute("""
                INSERT IGNORE INTO cars (brand, model, year, engine_size, mileage, price, url, image_url, country, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'LV', %s, %s)
            """, (actual_brand, model, year, engine_size, mileage, price, url_full, image_url, now, now))
            conn.commit()
            added += cursor.rowcount
        except Exception as e:
            print(f"DB kļūda ({brand}): {e}")
            conn.rollback()

    cursor.close()
    print(f"[{brand}] Ievietoti {added} ieraksti no {url}")
    return added, True


def scrape_brand(base_url, brand, slug):
    # Paginates through all listing pages for one ss.com brand and scrapes each.
    print(f"\n=== Sāku marku: {brand} ===")
    total_added = 0
    page = 1

    conn = get_db()
    try:
        while True:
            url = base_url if page == 1 else f"{base_url}page{page}.html"
            added, has_more = scrape_page(url, brand, slug, conn)
            total_added += added

            if not has_more:
                break

            page += 1
            time.sleep(random.uniform(0.5, 1.5))
    finally:
        conn.close()

    print(f"=== Marka '{brand}' pabeigta. Pievienoti {total_added} ieraksti. ===")
    return total_added


def scrape_ss():
    # Entry point for the Latvian scraper — scrapes all brands sequentially.
    print("\n=== Sāku ss.com (Latvija) ===")
    brand_urls = get_brand_urls()
    if not brand_urls:
        print("Nav atrasta neviena marka.")
        return 0

    total = 0
    for url, brand, slug in brand_urls:
        try:
            total += scrape_brand(url, brand, slug)
        except Exception as e:
            print(f"Kļūda ({brand}): {e}")

    print(f"=== ss.com pabeigts. Pievienoti {total} ieraksti. ===")
    return total


# ---------------------------------------------------------------------------
# autoportaal.ee (Estonia) scraper
# ---------------------------------------------------------------------------

AUTOPORTAAL_BASE = "https://autoportaal.ee/en/used-cars"


def split_brand_model(h2_text):
    # Splits "Opel Astra" into ("Opel", "Astra"). Handles known two-word brands like "Land Rover".
    text = h2_text.strip()
    for multi in MULTI_WORD_BRANDS:
        if text.startswith(multi + " "):
            return multi, text[len(multi):].strip()
    parts = text.split(" ", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return text, ""


def scrape_autoportaal_page(page_num, conn):
    # Scrapes one paginated page of autoportaal.ee and inserts found cars into the DB.
    url = f"{AUTOPORTAAL_BASE}?page={page_num}"
    print(f"Scrapo autoportaal.ee: {url}")

    try:
        time.sleep(random.uniform(0.5, 1.2))
        response = requests.get(url, headers=HEADERS, timeout=15)
    except Exception as e:
        print(f"Kļūda: {e}")
        return 0, False

    if response.status_code != 200:
        print(f"Kļūda: {response.status_code}")
        return 0, False

    soup = BeautifulSoup(response.text, "html.parser")

    containers = soup.find_all("div", class_="advertisementListContainer")
    if not containers:
        return 0, False

    cursor = conn.cursor()
    added = 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for container in containers:
        try:
            data = container.find("a", class_="dataArea")
            if not data:
                continue

            h2 = data.find("h2")
            if not h2:
                continue
            brand, model = split_brand_model(h2.get_text())

            ad_url = data["href"]

            # Image lives in the photoArea sibling
            img = container.find("img")
            image_url = img["src"] if img and img.get("src") else None

            # Price from the first finalPrice div
            price_div = data.find("div", class_="finalPrice")
            price = clean_int(price_div.get_text()) if price_div else None

            # Use additionalDataMobile which has proper li classes
            mobile = data.find("div", class_="additionalDataMobile")
            year_li = mobile.find("li", class_="year") if mobile else None
            mileage_li = mobile.find("li", class_="mileage") if mobile else None
            engine_li = mobile.find("li", class_="power_kw") if mobile else None

            year = year_li.get_text(strip=True) if year_li else None
            mileage = clean_int(mileage_li.get_text()) if mileage_li else None

            engine_size = None
            if engine_li:
                raw = engine_li.get_text(strip=True)  # "2.0, 81 kW"
                engine_size = raw.split(",")[0].strip() if raw and raw != "-" else None

            cursor.execute("""
                INSERT IGNORE INTO cars (brand, model, year, engine_size, mileage, price, url, image_url, country, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'EE', %s, %s)
            """, (brand, model, year, engine_size, mileage, price, ad_url, image_url, now, now))
            conn.commit()
            added += cursor.rowcount
        except Exception as e:
            print(f"DB kļūda (autoportaal row): {e}")
            conn.rollback()

    cursor.close()
    print(f"[EE] Ievietoti {added} ieraksti no {url}")
    return added, True


def scrape_autoportaal():
    # Entry point for the Estonian scraper — iterates all pages until no listings are found.
    print("\n=== Sāku autoportaal.ee (Igaunija) ===")
    total_added = 0
    page = 0

    conn = get_db()
    try:
        while True:
            added, has_more = scrape_autoportaal_page(page, conn)
            total_added += added
            if not has_more:
                break
            page += 1
    finally:
        conn.close()

    print(f"=== autoportaal.ee pabeigts. Pievienoti {total_added} ieraksti. ===")
    return total_added


# ---------------------------------------------------------------------------
# autogidas.lt (Lithuania) scraper
# ---------------------------------------------------------------------------

AUTOGIDAS_BASE = "https://autogidas.lt"
AUTOGIDAS_CARS = "https://autogidas.lt/en/skelbimai/automobiliai/"


def get_autogidas_brands():
    # Fetches the autogidas.lt cars page and returns a list of (url, slug, name) for each brand.
    print("Lasu autogidas.lt markas...")
    try:
        r = requests.get(AUTOGIDAS_CARS, headers=HEADERS_LT, timeout=15)
    except Exception as e:
        print(f"Kļūda: {e}")
        return []

    if r.status_code != 200:
        print(f"Kļūda: {r.status_code}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    brands = []
    seen = set()
    for a in soup.find_all("a", href=re.compile(r"^/en/skelbimai/automobiliai/[^/]+/$")):
        href = a["href"]
        slug = href.rstrip("/").split("/")[-1]
        if slug not in seen:
            seen.add(slug)
            brands.append((AUTOGIDAS_BASE + href, slug, a.get_text(strip=True)))

    print(f"Atrasts {len(brands)} autogidas.lt marku.")
    return brands


def scrape_autogidas_page(url, brand_name, conn):
    # Scrapes one autogidas.lt brand listing page and inserts found cars into the DB.
    print(f"Scrapo autogidas.lt: {url}")

    try:
        time.sleep(random.uniform(2.0, 4.0))
        r = requests.get(url, headers=HEADERS_LT, timeout=15)
    except Exception as e:
        print(f"Kļūda: {e}")
        return 0, False

    if r.status_code != 200:
        print(f"Kļūda: {r.status_code}")
        return 0, False

    soup = BeautifulSoup(r.text, "html.parser")
    items = soup.find_all("div", class_="article-item")
    if not items:
        return 0, False

    cursor = conn.cursor()
    added = 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for item in items:
        try:
            link = item.find("a", class_="item-link")
            if not link:
                continue
            ad_url = AUTOGIDAS_BASE + link["href"]

            h2 = item.find("h2", class_="item-title")
            title = h2.get_text(strip=True) if h2 else ""
            # Strip brand prefix to get model ("BMW 320" → "320")
            if title.lower().startswith(brand_name.lower()):
                model = title[len(brand_name):].strip()
            else:
                model = title

            img = item.find("img", class_="js-image")
            image_url = img["src"] if img and img.get("src") else None

            price_div = item.find("div", class_="item-price")
            price = clean_int(price_div.get_text()) if price_div else None

            params = [s.get_text(strip=True) for s in item.find_all("span", class_="parameter-value")]

            # Identify params by content — order varies between listings
            year = None
            mileage = None
            engine_size = None
            for p in params:
                if re.match(r"^\d{4}-\d{2}$", p):          # "2010-12"
                    year = p[:4]
                elif " km" in p:                             # "330 000 km"
                    mileage = clean_int(p)
                elif " L, " in p and "kW" in p:             # "2.0 L, 135 kW"
                    engine_size = p.split(" L")[0].strip()

            cursor.execute("""
                INSERT IGNORE INTO cars (brand, model, year, engine_size, mileage, price, url, image_url, country, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'LT', %s, %s)
            """, (brand_name, model, year, engine_size, mileage, price, ad_url, image_url, now, now))
            conn.commit()
            added += cursor.rowcount
        except Exception as e:
            print(f"DB kļūda (autogidas row): {e}")
            conn.rollback()

    cursor.close()
    print(f"[LT] Ievietoti {added} ieraksti no {url}")
    return added, True


def scrape_autogidas_brand(base_url, slug, brand_name):
    # Paginates through all listing pages for one autogidas.lt brand and scrapes each.
    print(f"\n=== autogidas.lt marka: {brand_name} ===")
    total_added = 0
    page = 1

    conn = get_db()
    try:
        while True:
            url = base_url if page == 1 else f"{base_url}?page={page}"
            added, has_more = scrape_autogidas_page(url, brand_name, conn)
            total_added += added
            if not has_more:
                break
            page += 1
            time.sleep(random.uniform(0.5, 1.2))
    finally:
        conn.close()

    print(f"=== autogidas.lt '{brand_name}' pabeigta. Pievienoti {total_added} ieraksti. ===")
    return total_added


def scrape_autogidas():
    # Entry point for the Lithuanian scraper — scrapes all brands sequentially (1 thread to avoid rate limiting).
    print("\n=== Sāku autogidas.lt (Lietuva) ===")
    brands = get_autogidas_brands()
    if not brands:
        print("Nav atrasta neviena marka.")
        return 0

    total = 0
    for url, slug, name in brands:
        try:
            total += scrape_autogidas_brand(url, slug, name)
        except Exception as e:
            print(f"Kļūda ({name}): {e}")

    print(f"=== autogidas.lt pabeigts. Pievienoti {total} ieraksti. ===")
    return total


# ---------------------------------------------------------------------------

def main():
    # Runs all three scrapers in sequence: Latvia (ss.com), Estonia (autoportaal.ee), Lithuania (autogidas.lt).
    print("Sākam datu vākšanu...")

    print("\n--- LATVIJA (ss.com) ---")
    total_lv = scrape_ss()

    print("\n--- IGAUNIJA (autoportaal.ee) ---")
    total_ee = scrape_autoportaal()

    print("\n--- LIETUVA (autogidas.lt) ---")
    total_lt = scrape_autogidas()

    print(f"\nGatavs. LV: {total_lv}, EE: {total_ee}, LT: {total_lt}")


if __name__ == "__main__":
    main()
