import requests
from bs4 import BeautifulSoup

# URL of the webpage to scrape
url = "https://www.ss.com/lv/transport/cars/mitsubishi/evolution/"

# Headers to mimic a browser request
headers = {
"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Send request to fetch the HTML content
response = requests.get(url, headers=headers)

# Check if request was successful
if response.status_code == 200:
    soup = BeautifulSoup(response.text, "html.parser")

# Find all mileage ("Nobraukums") values
mileage_elements = soup.find_all("td", class_="msga2-r pp6")

# Extract and print the mileage values
print("Nobraukums values found:")
for element in mileage_elements:
    print(element.get_text(strip=True)) # Proper indentation here
else:
    print(f"Failed to fetch the webpage. Status Code: {response.status_code}")
