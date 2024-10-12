import requests
from bs4 import BeautifulSoup
import re
from difflib import SequenceMatcher
import os
def getIMDBID(name):
    omdb_api_key = os.environ.get('OMDB_API_KEY')
    url = f"http://www.omdbapi.com/?t={name.strip()}&apikey={omdb_api_key}&plot=full&r=json"
    res = requests.get(url).json()

    if res.get("Response") != 'False':
        return res.get("imdbID")
    else:
        print("Couldn't find IMDB ID")
        return None

def getDesc(soup, s):
    descs = soup.findAll("h5", {"class": "details-title"})
    for desc in descs:
        if s == desc.text.strip():
            parent = desc.parent
            text = parent.find("div", {"class": "details-body"}).p
            return text.text.strip() if text else ""
    return ""

def DoveFoundationScrapper(videoName):
    sURL = f'https://dove.org/search/reviews/{videoName.replace(" ", "+")}'
    s = requests.Session()
    r = s.get(sURL)

    Cats = {0: "None", 1: "Mild", 2: "Moderate", 3: "Severe"}

    if r.status_code != 200:
        print(f"Failed to fetch search results. Status code: {r.status_code}")
        return create_failed_review(videoName)

    sSoup = BeautifulSoup(r.text, "html.parser")
    res = sSoup.find("div", {"class": "movie-cards search-cards"})

    if not res or "Nothing matches your search term" in str(res):
        print("No results found")
        return create_failed_review(videoName)

    try:
        resURL = res.find("a")["href"]
        response = s.get(resURL)
        soup = BeautifulSoup(response.text, "html.parser")
        title = soup.title.text.replace("- Dove.org", "").strip()

        checktitle = re.sub(r'[^a-zA-Z0-9]', '', title.lower())
        checkvideoName = re.sub(r'[^a-zA-Z0-9]', '', videoName.lower())

        if checkvideoName not in checktitle:
            print(f"Dove returned wrong media: {title}")
            return create_failed_review(videoName)

        table = soup.find("div", {"class": "matrix-categories"})
        items = table.findAll("span", {"class": "item-text"})
        sections = table.findAll("span", {"class": "categories-item"})
        descs = soup.find("div", {"class": "main-content details-wrap"})

        Details = []
        for item, section in zip(items, sections):
            try:
                ID = int(section["class"][1].replace("categories-item--", "").strip())
                CatData = {
                    "name": item.text.strip(),
                    "score": str(ID),
                    "description": getDesc(descs, item.text.strip()),
                    "cat": Cats[ID],
                    "votes": None
                }
                Details.append(CatData)
            except (KeyError, ValueError):
                print(f"Failed to process category: {item.text.strip()}")

        return {
            "id": getIMDBID(title),
            "status": "Success",
            "title": title.title(),
            "provider": "DoveFoundation",
            "recommended-age": None,
            "review-items": Details,
            "review-link": resURL
        }

    except Exception as e:
        print(f"Error processing Dove Foundation review: {str(e)}")
        return create_failed_review(videoName)

def create_failed_review(videoName):
    return {
        "id": None,
        "status": "Failed",
        "title": videoName.title(),
        "provider": "DoveFoundation",
        "recommended-age": None,
        "review-items": None,
        "review-link": None
    }
