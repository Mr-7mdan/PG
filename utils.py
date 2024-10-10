import requests
import json
import os
from flask import current_app as app
from SQLiteCache import SqliteCache

# Initialize the database connection
db = SqliteCache('cache.sqlite')

# Add this function to get movie/TV show name from OMDB API
def get_title_from_omdb(imdb_id):
    omdb_api_key = os.environ.get('OMDB_API_KEY')
    if not omdb_api_key:
        app.logger.error("OMDB API key not found in environment variables")
        return None

    cache_key = f"omdb_title_{imdb_id}"
    cached_data = db.get_omdb_cache(cache_key)
    if cached_data:
        app.logger.info(f"Retrieved title for {imdb_id} from OMDB cache")
        return cached_data.get('Title')

    url = f"http://www.omdbapi.com/?i={imdb_id}&apikey={omdb_api_key}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if data.get('Response') == 'True':
            db.set_omdb_cache(cache_key, data)
            return data.get('Title')
        else:
            app.logger.warning(f"No title found for IMDb ID: {imdb_id}")
            return None
    except requests.RequestException as e:
        app.logger.error(f"Error fetching data from OMDB: {str(e)}")
        return None

def get_imdb_id_from_omdb(video_name, release_year=None):
    omdb_api_key = os.environ.get('OMDB_API_KEY')
    if not omdb_api_key:
        app.logger.error("OMDB API key not found in environment variables")
        return None

    cache_key = f"omdb_id_{video_name}_{release_year}"
    cached_data = db.get_omdb_cache(cache_key)
    if cached_data:
        app.logger.info(f"Retrieved data for {video_name} ({release_year}) from OMDB cache")
        return cached_data

    url = f"http://www.omdbapi.com/?t={video_name}&y={release_year}&apikey={omdb_api_key}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if data.get('Response') == 'True':
            db.set_omdb_cache(cache_key, data)
            return data
        else:
            app.logger.warning(f"No IMDB data found for: {video_name}")
            return None
    except requests.RequestException as e:
        app.logger.error(f"Error fetching data from OMDB: {str(e)}")
        return None