import requests
from bs4 import BeautifulSoup
import re
import json

def cringMDBScraper(ID,videoName):
    Session = requests.Session()
    strName = videoName.replace(":", "").replace(" ","+").replace("%3A","").lower()
    url = 'https://cringemdb.com/search?term=' + strName
    print(url)
    r = Session.get(url)
    Results = r.json()
    print(Results)
    advisory,show_info = [],[]
    Cats = {
        "no": "None",
        "yes": "Moderate"
        }
    for res in Results:
        print("running for" + str(res))
        moviename = res["movie"]
        moviename1 = re.sub(r'\(\d*\)','', moviename).strip()
        moviename = moviename1.replace(":", "").replace("%3A","").replace(" ","+").lower()
        print("moviename : " + moviename)
        print("videoName : " + videoName)
        if videoName == moviename:
            slug = res["slug"]
            movieURL = 'https://cringemdb.com/movie/' + slug
            r = Session.get(movieURL)
            if '200' in str(r):
                Soup = BeautifulSoup(r.text, "html.parser")
                SectionsSoup = Soup.find("div", {"class":"content-warnings"})
                Sections = SectionsSoup.findAll("div",{"class":"content-flag"})

                votesSoup = Soup.find("div",{"class":"movie-info"})
                votes = votesSoup.find("span",{"itemprop":"bestRating"}).text
                for sec in Sections:
                    section = {
                        "name": sec.h3.text.strip(),
                        "cat": Cats[sec.h4.text.lower().strip()],
                        "votes": votes.strip()
                    }
                    advisory.append(section)

                show_info = {
                    "id": ID,
                    "status": "Sucess",
                    "title": moviename1,
                    "provider": "cringMDB",
                    "recommended-age": None,
                    "review-items": advisory,
                    "review-link": movieURL
                        }

    if advisory in [None,""]:
        show_info = {
            "id": ID,
            "status": "Failed",
            "title": videoName,
            "provider": "cringMDB",
            "recommended-age": None,
            "review-items": None,
            "review-link": None
                }
    return show_info
