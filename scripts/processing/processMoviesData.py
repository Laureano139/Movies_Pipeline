
import json
import os
from dotenv import load_dotenv

load_dotenv()

""""
TODO: 
1. Load the raw data from the JSON files.
2. Extract relevant fields (e.g., title, release date, rating, overview).
3. Handle missing data.
4. Edit the vote_average to a .1f format so we can try to implement a star rating system in the future.
5. Save the cleaned data to a new JSON file for further analysis.
"""

API_KEY = os.getenv("API_KEY")
RAW_DATA_DIR = "../../data/raw"

IMAGES_BASE_URL = "https://image.tmdb.org/t/p/w500/"
MOVIE_GENRES_URL = "https://api.themoviedb.org/3/genre/movie/list?api_key=" + API_KEY
TV_GENRES_URL = "https://api.themoviedb.org/3/genre/tv/list?api_key=" + API_KEY