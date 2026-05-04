# My Baltic Car — Scraper

Python scraper that collects used car listings from three Baltic sources and stores them in MySQL.

---

## Sources

| Site | Country | Notes |
|------|---------|-------|
| ss.com/lv/transport/cars/ | Latvia | Table-based listings |
| autoportaal.ee/en/used-cars | Estonia | Card-based listings, ~519 pages |
| autogidas.lt/en/skelbimai/automobiliai/ | Lithuania | Brand pages, 20 top brands |

---

## Requirements

```bash
pip install requests beautifulsoup4 mysql-connector-python
```

---

## Database setup

Create the database and run Laravel migrations first:

```sql
CREATE DATABASE sscarsdb;
```

```bash
cd ../nosl-d-pt22 && php artisan migrate
```

---

## Usage

```bash
cd webscraper_testing

# Run all three countries
python3 scraper.py

# Run one country at a time
python3 -c "from scraper import scrape_ss; scrape_ss()"                      # Latvia
python3 -c "from scraper import scrape_autoportaal; scrape_autoportaal()"   # Estonia
python3 -c "from scraper import scrape_autogidas; scrape_autogidas()"        # Lithuania
```

To clear and re-scrape a country:
```sql
DELETE FROM cars WHERE country = 'EE';
```

---

## Known issues

- **autogidas.lt rate limiting (429)** — runs with 1 thread and 2–4s delays. If blocked mid-run, wait and re-run; `INSERT IGNORE` skips already-scraped listings.
- **ss.com category pages** — electric-cars, sport-cars, tuned-cars etc. have an extra brand column. Handled via `CATEGORY_SLUGS`.
- **autoportaal.ee** — must use `.webp` image URLs from the listing card directly; no need to visit detail pages.
