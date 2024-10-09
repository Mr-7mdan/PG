import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor
import logging
import json
from curl_cffi import requests
import random
import time
import os
import traceback

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a session object
session = requests.Session()

IMPERSONATE_OPTIONS = [
    "chrome110", "chrome107", "chrome104", "chrome99", "chrome100", 
    "chrome101", "chrome120", "edge99", "edge101", "safari15_3", "safari15_5"
]

USER_AGENTS = {
    "chrome110": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    "chrome107": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
    "chrome104": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
    "chrome99": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36",
    "chrome100": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36",
    "chrome101": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36",
    "chrome120": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "edge99": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36 Edg/99.0.1150.30",
    "edge101": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36 Edg/101.0.1210.39",
    "safari15_3": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15",
    "safari15_5": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15"
}

def get_scenes(section):
    logger.info(f"Getting scenes from section: {section.get('id', 'Unknown section')}")
    scenes_raw = section.find_all('li', class_='ipc-zebra-list__item')
    logger.info(f"Found {len(scenes_raw)} raw scenes")
    scenes = []
    for scene in scenes_raw:
        # Exclude voting elements
        if not scene.find(class_='advisory-severity-vote__container'):
            scene_text = scene.text.strip()
            scene_text = scene_text.replace('Edit', '').strip()
            scenes.append(scene_text)
    logger.info(f"Processed {len(scenes)} scenes")
    return scenes

@lru_cache(maxsize=128)
def get_cat(section):
    logger.info(f"Getting category for section: {section.get('id', 'Unknown section')}")
    vote_container = section.find(class_='advisory-severity-vote__container')
    
    if not vote_container:
        logger.warning("No vote container found")
        return None, None, None, None
    
    span = vote_container.find('span', class_='ipl-status-pill')
    if not span:
        logger.warning("No span found in vote container")
        return None, None, None, None
    
    cat = span.text
    logger.info(f"Found category: {cat}")
    
    a_tag = vote_container.find('a', class_='advisory-severity-vote__message')
    if not a_tag:
        logger.warning("No a tag found in vote container")
        return cat, None, None, None
    
    vote = a_tag.text
    pattern = r'([\d,]+)\s+of\s+([\d,]+)'
    m = re.match(pattern, vote)
    if not m:
        logger.warning(f"Could not parse vote string: {vote}")
        return cat, None, None, None
    
    vote = int(m[1].replace(',', ''))
    outof = int(m[2].replace(',', ''))
    percent = round((vote/outof) * 100)
    logger.info(f"Parsed vote: {vote} of {outof} ({percent}%)")
    return cat, vote, outof, percent

def get_episode_info(soup):
    logger.info("Getting episode info")
    episode_info = soup.find("div", class_="episode-info")
    if episode_info:
        episode_title = episode_info.find("h3").text.strip()
        episode_number = episode_info.find("div", class_="ipc-metadata-list-item__content-container").text.strip()
        logger.info(f"Found episode info: {episode_title} ({episode_number})")
        return f"{episode_title} ({episode_number})"
    logger.info("No episode info found")
    return None

def fetch_url(url, max_retries=5):
    impersonate_option = random.choice(IMPERSONATE_OPTIONS)
    user_agent = USER_AGENTS[impersonate_option]

    headers = {
        'User-Agent': user_agent,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.imdb.com/',
        'Origin': 'https://www.imdb.com',
        'DNT': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
    }

    # logger.info(f"Using User-Agent: {user_agent}")
    # logger.info(f"Impersonating: {impersonate_option}")

    for attempt in range(max_retries):
        try:
            response = session.get(
                url, 
                headers=headers, 
                impersonate=impersonate_option
            )
            logger.info(f"Response status code: {response.status_code}")
            if response.status_code == 200:
                # Export response text to file
                # with open('last_response.html', 'w', encoding='utf-8') as f:
                #     f.write(response.text)
                # logger.info("Response text exported to last_response.html")
                return response.text
            else:
                logger.error(f"Attempt {attempt + 1} failed. Status code: {response.status_code}")
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed. Error: {str(e)}")

        if attempt < max_retries - 1:  # Don't sleep after the last attempt
            if attempt == 0:
                sleep_time = 0  # No delay for the first retry
            elif attempt == 1:
                sleep_time = random.uniform(1, 3)
            elif attempt == 2:
                sleep_time = random.uniform(5, 10)
            elif attempt == 3:
                sleep_time = random.uniform(11, 20)
            else:
                sleep_time = random.uniform(20, 40)
            
            logger.info(f"Retrying in {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)

    logger.error(f"Failed to fetch URL: {url} after {max_retries} attempts.")
    return None

def imdb_parentsguide(tid, videoName):
    logger.info(f"Processing IMDB parents guide for {tid}: {videoName}")
    pg_url = f'https://www.imdb.com/title/{tid}/parentalguide'
    
    html = fetch_url(pg_url)
    if html is None:
        logger.error(f"Failed to fetch URL: {pg_url}")
        return {
            "id": tid,
            "status": "Failed",
            "title": videoName,
            "provider": "imdb",
            "review-items": None,
            "review-link": pg_url,
            "is_episode": False,
            "series_id": None
        }
    
    soup = BeautifulSoup(html, 'lxml')

    # Check if it's the new page structure
    new_structure = soup.find("main", role="main")
    old_structure = soup.find("div", id="main")

    try:
        if new_structure and not old_structure:
            logger.info("Processing new page structure")
            return process_new_structure(soup, tid, videoName, pg_url)
        elif old_structure and not new_structure:
            logger.info("Processing old page structure")
            return process_old_structure(soup, tid, videoName, pg_url)
        else:
            logger.warning("Unclear page structure, defaulting to old structure")
            return process_old_structure(soup, tid, videoName, pg_url)
    except Exception as e:
        logger.error(f"Error processing IMDB parents guide: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {
            "id": tid,
            "status": "Failed",
            "title": videoName,
            "provider": "imdb",
            "review-items": None,
            "review-link": pg_url,
            "is_episode": False,
            "series_id": None
        }

def process_new_structure(soup, tid, videoName, pg_url):
    logger.info("Processing new page structure")
    
    # Find the script tag containing the JSON data
    script_tag = soup.find('script', {'id': '__NEXT_DATA__', 'type': 'application/json'})
    if not script_tag:
        logger.warning("JSON data not found in script tag")
        return create_error_result(tid, videoName, pg_url)

    # Parse the JSON data
    try:
        json_data = json.loads(script_tag.string)
        # # Export JSON script to file
        # with open('script.json', 'w', encoding='utf-8') as f:
        #     json.dump(json_data, f)
        content_data = json_data['props']['pageProps']['contentData']
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Error parsing JSON data: {str(e)}")
        return create_error_result(tid, videoName, pg_url)

    # Extract title
    videoName = content_data.get('entityMetadata', {}).get('titleText', {}).get('text', videoName)
    # logger.info(f"Extracted title: {videoName}")

    # Check if it's an episode
    is_episode = content_data.get('entityMetadata', {}).get('titleType', {}).get('isEpisode', False)
    series_id = content_data.get('entityMetadata', {}).get('series', {}).get('id', tid) if is_episode else None
    # logger.info(f"Is episode: {is_episode}, Series ID: {series_id}")

    # Process categories
    advisory = []
    # parents_guide = content_data.get('parentsGuide', {})
    categories = content_data.get('categories', [])
    
    if not categories:
        logger.warning("No categories found in parentsGuide")
        return create_error_result(tid, videoName, pg_url)

    for category in categories:
        result = process_category(category)
        if result:
            advisory.append(result)

    status = "Success" if advisory else "Failed"
    if status == "Failed":
        logger.warning(f"No advisory items found for {tid}: {videoName}")

    result = {
        "id": tid,
        "status": status,
        "title": videoName,
        "provider": "imdb",
        "review-items": advisory if advisory else None,
        "review-link": pg_url,
        "is_episode": is_episode,
        "series_id": series_id if is_episode else None
    }

    # logger.info(f"IMDB result for {tid}: {json.dumps(result, indent=2)}")
    return result

def process_category(category):
    name = category.get('title', 'Unknown')
    # logger.info(f"Processing category: {name}")

    severity = category.get('severitySummary', {}).get('text', 'Unknown')
    # logger.info(f"Category severity: {severity}")

    items = category.get('items', [])
    descriptions = []
    for item in items:
        desc = clean_text(item.get('text', ''))
        if desc:
            descriptions.append(desc)
    
    description = "\n\n".join(descriptions)
    # logger.info(f"Number of descriptions: {len(descriptions)}")

    votes = f"{category.get('totalSeverityVotes', 'N/A')} votes"
    # logger.info(f"Category votes: {votes}")

    return {
        'name': name,
        'description': description,
        'cat': severity,
        'votes': votes
    }

def create_error_result(tid, videoName, pg_url):
    return {
        "id": tid,
        "status": "Failed",
        "title": videoName,
        "provider": "imdb",
        "review-items": None,
        "review-link": pg_url,
        "is_episode": False,
        "series_id": None
    }

def process_old_structure(soup, tid, videoName, pg_url):
    logger.info("Processing old page structure")
    
    main_content = soup.find("div", id="main")
    if not main_content:
        logger.warning("Main content section not found in old structure")
        return {
            "id": tid,
            "status": "Failed",
            "title": videoName,
            "provider": "imdb",
            "review-items": None,
            "review-link": pg_url,
            "is_episode": False,
            "series_id": None
        }

    # Extract title
    title_elem = soup.find("h3", itemprop="name")
    if title_elem:
        videoName = title_elem.text.strip()

    # Check if it's an episode
    episode_info = get_episode_info(soup)
    is_episode = episode_info is not None
    series_id = tid

    if is_episode:
        series_link = soup.find("div", class_="titleParent").find("a")
        if series_link:
            series_id = series_link['href'].split('/')[2]

    # Process sections
    advisory = []
    section_ids = ['advisory-nudity', 'advisory-violence', 'advisory-profanity', 'advisory-alcohol', 'advisory-frightening']
    
    for section_id in section_ids:
        section = soup.find("section", id=section_id)
        if section:
            result = process_old_section(section)
            if result:
                advisory.append(result)

    status = "Success" if advisory else "Failed"
    if status == "Failed":
        logger.warning(f"No advisory items found for {tid}: {videoName}")

    result = {
        "id": tid,
        "status": status,
        "title": videoName,
        "provider": "imdb",
        "review-items": advisory if advisory else None,
        "review-link": pg_url,
        "is_episode": is_episode,
        "series_id": series_id if is_episode else None
    }

    # logger.info(f"IMDB result for {tid}: {json.dumps(result, indent=2)}")
    return result

def process_old_section(section):
    name = section.find("h4", class_="ipl-list-title")
    name = name.text.strip() if name else "Unknown"
    # logger.info(f"Processing section: {name}")

    severity_container = section.find("div", class_="advisory-severity-vote__container")
    severity = "Unknown"
    if severity_container:
        severity_pill = severity_container.find("span", class_="ipl-status-pill")
        severity = severity_pill.text.strip() if severity_pill else "Unknown"
    # logger.info(f"Section severity: {severity}")

    items = section.find_all("li", class_="ipl-zebra-list__item")
    descriptions = []
    for item in items:
        if not item.find(class_='advisory-severity-vote'):
            desc = clean_text(item.text)
            if desc:
                descriptions.append(desc)
    
    description = "\n\n".join(descriptions)
    # logger.info(f"Number of descriptions: {len(descriptions)}")

    votes = "N/A"
    if severity_container:
        vote_message = severity_container.find("a", class_="advisory-severity-vote__message")
        votes = vote_message.text if vote_message else "N/A"
    # logger.info(f"Section votes: {votes}")

    return {
        'name': name,
        'description': description,
        'cat': severity,
        'votes': votes
    }

def clean_text(text):
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Remove extra whitespace and newline characters
    text = re.sub(r'\s+', ' ', text).strip()
    return text