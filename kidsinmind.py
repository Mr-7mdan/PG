import requests
from bs4 import BeautifulSoup
import re
from difflib import SequenceMatcher
import logging

logger = logging.getLogger(__name__)

def string_similarity(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def KidsInMindScraper(ID, videoName, release_year=None):
    Session = requests.Session()
    videoName = videoName.replace(":", "%3A").replace(" ","+")
    sURL = 'https://kids-in-mind.com/search-desktop.htm?fwp_keyword=' + videoName
    url = sURL
    r = Session.get(url)
    Cats = {
        0: "None",
        1: "Clean",
        2: "Mild",
        3: "Mild",
        4: "Mild",
        5: "Moderate",
        6: "Moderate",
        7: "Moderate",
        8: "Severe",
        9: "Severe",
        10: "Severe",
    }
    NamesMap = {
        "SEX/NUDITY" : "Sex & Nudity",
        "VIOLENCE/GORE": "Violence",
        "LANGUAGE":"Profanity",
        "SUBSTANCE USE":"Smoking, Alchohol & Drugs",
        "DISCUSSION TOPICS": "Discussion Topics",
        "MESSAGE":"Message",
    }
    AcceptedNames = ['SEX/NUDITY','VIOLENCE/GORE','LANGUAGE','SUBSTANCE USE','DISCUSSION TOPICS','MESSAGE']
    Details = []
    CatData = []
    sURLs = []
    if '200' in str(r):
        sSoup = BeautifulSoup(r.text, "html.parser")
        res = sSoup.find("div", {"class":"facetwp-template"})
        sResults = res.findAll("a")
        print("found " + str(len(sResults)) + " results for " + videoName)
        for sRes in sResults:
            sURLs.append(sRes["href"])
        print(sURLs[0])
        NoRes = re.compile("Nothing matches your search term").findall(str(res))
        print(NoRes)
        if len(NoRes) == 0:
            logger.info("Skipped No Results Phase")
            for k in range(0,len(sURLs)):
                if 'https://kids-in-mind.com' not in sURLs[k]:
                    sURLs[k] = 'https://kids-in-mind.com' + sURLs[k]

                resURL = sURLs[k]
                logger.info(f"KidsInMind trying .. {resURL}")
                response = Session.get(resURL)
                soup = BeautifulSoup(response.text, "html.parser")

                sPattern3 = r"href.*imdb.*title.(.*?)\/"
                imdbid = str(re.compile(sPattern3).findall(str(soup)))

                # Extract title from the <title> tag
                title_tag = soup.find('title')
                if title_tag:
                    full_title = title_tag.text.strip()
                    # The title now follows the format: "Movie Title [Year] [Rating] - 7.7.7"
                    title_parts = full_title.split('[')
                    page_title = title_parts[0].strip()
                    page_year = title_parts[1].strip(']').strip() if len(title_parts) > 1 else None
                    # Clean page_year to remove any non-digit characters
                    page_year = ''.join(filter(str.isdigit, page_year)) if page_year else None
                    logger.info(f"Extracted title: '{page_title}', year: '{page_year}'")
                else:
                    page_title = None
                    page_year = None
                    logger.warning("Failed to extract title from the page")

                # Check if IMDB ID matches
                if ID and ID in imdbid:
                    logger.info(f"IMDB ID match found: {ID}")
                    match_found = True
                else:
                    # Fallback to string similarity
                    if page_title:
                        similarity = string_similarity(videoName.replace("+", " "), page_title)
                        logger.info(f"String similarity: {similarity:.2f} for '{videoName.replace('+', ' ')}' vs '{page_title}'")
                        
                        # If release year is provided, check if it matches
                        if release_year and page_year:
                            try:
                                if str(release_year) == page_year:
                                    similarity += 0.2
                                    logger.info(f"Year exact match. Boosted similarity: {similarity:.2f}")
                                elif abs(int(release_year) - int(page_year)) <= 1:
                                    similarity += 0.1
                                    logger.info(f"Year off by 1. Boosted similarity: {similarity:.2f}")
                                else:
                                    logger.info(f"Year mismatch. Release year: {release_year}, Page year: {page_year}")
                            except ValueError:
                                logger.warning(f"Could not compare years. Release year: {release_year}, Page year: {page_year}")
                        
                        if similarity > 0.8:  # You can adjust this threshold
                            logger.info(f"Match found based on title similarity: {page_title} ({page_year})")
                            match_found = True
                        else:
                            logger.info(f"No match found. Similarity {similarity:.2f} below threshold 0.8")
                            match_found = False
                    else:
                        logger.warning("No page title found for similarity check")
                        match_found = False

                if match_found:
                    logger.info(f"Processing match: {page_title}")
                    title = page_title

                    ratingstr = soup.title.string
                    sPattern =  r"(\d)\.(\d)\.(\d)"
                    aMatches = re.compile(sPattern).findall(ratingstr)

                    try:
                        NudeRating = round(int(aMatches[0][0])/2)
                    except:
                        NudeRating = 0

                    blocks = soup.findAll("div",{"class":"et_pb_text_inner"})
                    i=1
                    for block in blocks:
                        if block.p is not None and i <=7:
                            items = block.findAll("h2")
                            if len(items) < 1:
                                items = block.findAll("span")

                            for item in items:
                                xitem = item.text.replace(title,"").strip()
                                itemtxt = ''.join((x for x in xitem if not x.isdigit())).strip()
                                if itemtxt in AcceptedNames:
                                    for x in xitem:
                                        if x.isdigit():
                                            ratetxt= int(''.join(x))
                                        else:
                                            ratetxt = 0
                                    parent = item.parent
                                    try:
                                        desc = parent.p.text
                                    except:
                                        desc = parent.text

                                    if block:
                                        CatData = {
                                                "name" : NamesMap[itemtxt],
                                                "score": int(ratetxt)/2,
                                                "description": desc,
                                                "cat": Cats[ratetxt],
                                                "votes": None
                                            }
                                        Details.append(CatData)
                        i = i +1

                    Review = {
                        "id": ID or imdbid.replace("['","").replace("']",""),
                        "title": title,
                        "provider": "KidsInMind",
                        "recommended-age": None,
                        "review-items": Details,
                        "review-link": resURL,
                    }
                    break
            else:
                logger.warning("No match found in any of the search results")
                Review = None
        else:
            logger.warning("No search results found")
            Review = {
                "id": ID,
                "status": "Failed",
                "title": videoName.replace("+", " "),
                "provider": "KidsInMind",
                "recommended-age": None,
                "review-items": None,
                "review-link": None,
            }
    else:
        logger.error(f"Failed to fetch search results. Status code: {r.status_code}")
        Review = {
            "id": ID,
            "status": "Failed",
            "title": videoName.replace("+", " "),
            "provider": "KidsInMind",
            "recommended-age": None,
            "review-items": None,
            "review-link": None,
        }

    return Review