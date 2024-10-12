import requests
from bs4 import BeautifulSoup
import re
import json
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

def MovieGuideOrgScrapper(ID, videoName):
    moviename = videoName.lower().strip().replace(" ","-").replace(":","").strip()

    ##search for the movie 1st
    URL = 'https://www.movieguide.org/reviews/' + moviename + '.html'
    print(URL)
    s = requests.Session()
    r = s.get(URL)

    Cats = {
        0: "None",
        1: "Mild",
        2: "Moderate",
        3: "Severe"
    }

    NamesMap = {
        "Dominant Worldview and Other Worldview Content/Elements:": "Dominant Worldview",
        "Foul Language": "Language",
        "Language": "Profanity",
        "Violence" : "Violence",
        "Nudity" : "Sex & Nudity",
        "Sex": "Sex & Nudity",
        "Alcohol Use" : "Smoking, Alchohol & Drugs",
        "Smoking and/or Drug Use and Abuse" : "Smoking, & Alchohol & Drugs",
        "Miscellaneous Immorality" : "Miscellaneous Immorality",
        "Making Love":"Sex & Nudity",
        "Nudity":"Sex & Nudity"
        }

    Details = []
    CatData = []

    def getMGDesc(descs, s):
        for i in range(0,len(descs)):
        #for desc in descriptions:
            if str(s) in [descs[i].text.replace(":","").strip()]:
                #print("found : requiring " + s + ", match with" + str(i) + "," + descs[i].text.replace(":","").strip())
                return descs[i].next_sibling.strip()
            #else:
               # print("Not found : requiring " + s + ", match with"  + str(i) + "," +  descs[i].text.replace(":","").strip())



    if '200' in str(r):
        Soup = BeautifulSoup(r.text, "html.parser")
        descriptions = Soup.find("div",{"class":"movieguide_review_content"}).findAll("div",{"class":"movieguide_subheading"})
        title = Soup.title.text.split("|")[0].split("-")[0].strip()
        #print(sSoup)
        classifications = Soup.find("table", {"class":"movieguide_content_summary"})
        matches = classifications.findAll("tr")
        #print(matches)
        for match in matches:
            #print(match.text)
            #if match.text.strip() in ["Nudity"]:
            if match.text.replace("\n","").strip() != 'NoneLightModerateHeavy':
                #print(match.text)
                ele = match.findAll("div")
                #print(ele)
                for i in range(0,4):
                    sPattern =  "movieguide_circle_red"
                    aMatches = re.compile(sPattern).findall(str(ele[i]))
                    sPattern2 =  "movieguide_circle_green"
                    bMatches = re.compile(sPattern2).findall(str(ele[i]))

                    if aMatches or bMatches:
                        CatData = {
                        "name" : NamesMap[str(match.text.replace("\n","").strip())],
                        "score": i,
                        "description": getMGDesc(descriptions, match.text.replace("\n","").strip()),
                        "cat": Cats.get(i),
                        "votes": None
                        }
                    #print(CatData)
                Details.append(CatData)
                    #print(Cats[i])
                    #print("-------------------------------------------------------")

        #print(Details)

        Review = {
            "id": getIMDBID(videoName),
            "status" : "Sucess",
            "title": title.title(),
            "provider": "MovieGuide",
            "recommended-age": None,
            "review-items": Details,
            "review-link": URL
        }
    return Review

