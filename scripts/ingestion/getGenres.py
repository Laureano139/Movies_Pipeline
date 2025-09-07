import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
OUTPUT_PATH = "data/raw"

moviesURL = f"https://api.themoviedb.org/3/genre/movie/list?api_key={API_KEY}"
response = requests.get(moviesURL)
data = response.json()

with open(os.path.join(OUTPUT_PATH, "movieGenres.json"),"w", encoding="utf-8") as f:
    json.dump(data, f, indent=4)
    
tvURL = f"https://api.themoviedb.org/3/genre/tv/list?api_key={API_KEY}"

response = requests.get(tvURL)
data = response.json()

with open(os.path.join(OUTPUT_PATH, "tvGenres.json"),"w", encoding="utf-8") as f:
    json.dump(data, f, indent=4)