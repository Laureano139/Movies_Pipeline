
import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_URL = "https://api.themoviedb.org/3"
OUTPUT_DIR = "data/raw"

def fetch_movies_data(endpoint: str, output_file: str, pages: int = 10):
    data = []
    for page in range(1, pages + 1):
        url = f"{BASE_URL}/{endpoint}?api_key={API_KEY}&language=en-US&page={page}"

        try:
            response = requests.get(url)
            response.raise_for_status()
            
            page_data = response.json()
            data.extend(page_data.get('results', []))
            print(f"  Page {page}/{pages} fetched.")

        except requests.exceptions.HTTPError as err:
            print(f"HTTP error: {err}")
            break
        except Exception as err:
            print(f"ERROR: {err}")
            break

    if data:
        with open(os.path.join(OUTPUT_DIR, output_file), 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Data successfully saved to '{os.path.join(OUTPUT_DIR, output_file)}'.")
    else:
        print("No data was obtained.")

if __name__ == "__main__":
    fetch_movies_data("movie/popular", "movies_popular.json")
    fetch_movies_data("tv/popular", "tv_popular.json")