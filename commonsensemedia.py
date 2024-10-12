import requests
from bs4 import BeautifulSoup
import re
import json


def CommonSenseScrapper(ID, videoName):
    CatsIDs = {
        0: "Clean",
        1: "Mild",
        2: "Moderate",
        3: "Moderate",
        4: "Severe",
        5: "Severe"
    }

    NamesMap = {
        "Positive Messages" :"Positive Messages",
        "Positive Role Models" :"Positive Role Models",
        "Diverse Representations" : "Diverse Representations",
        "Violence & Scariness" : "Violence",
        "Sex, Romance & Nudity" : "Sex & Nudity",
        "Language" : "Profanity",
        "Products & Purchases" : "Products & Purchases",
        "Drinking, Drugs & Smoking" : "Smoking, Alchohol & Drugs",
        "Educational Value":"Educational Value"
        }
    movie_id = videoName.replace(":","").replace(" ","-")
    movie_url = "https://www.commonsensemedia.org" + "/movie-reviews/" + str(movie_id)
    print(movie_url)
    response = requests.get(movie_url)

    if '200' in str(response):
        soup = BeautifulSoup(response.text, "html.parser")
        Cats = soup.find("div",{"id":"review-view-content-grid"}).find("div",{"class":"row"}).findAll("span",{"class":"rating__label"})
        age = soup.find("div", {"class": "review-rating"}).find("span", {"class":"rating__age"}).text.strip()
        review_summary = soup.find("div", {"class": "review-view-summary"})
        jsonData = soup.find('script',{"type":"application/ld+json"}).string
        #print(jsonData)
        jsonload = json.loads(jsonData)
        title = jsonload["@graph"][0]["itemReviewed"]["name"]
        imdburl= jsonload["@graph"][0]["itemReviewed"]["sameAs"]
        age2 = "age"+jsonload["@graph"][0]["typicalAgeRange"]
        isFamilyFriendly = "age"+jsonload["@graph"][0]["isFamilyFriendly"]
        datePublished = "age"+jsonload["@graph"][0]["datePublished"]
        sPattern3 = r"http.*imdb.*title.(.*?)\/"
        imdbid = str(re.compile(sPattern3).findall(str(imdburl))).replace("[","").replace("]","").replace("'","")
        #print()
        Details = []
        namePattern = re.compile(r'data-text="(.*\n*?)')
        CatData = []
        for cat in Cats:
            #print(cat.text)
            descparenttag = cat.parent.parent
            try:
                desc = re.findall(namePattern,  str(descparenttag))[0].strip().replace("&lt;","").replace("p&gt;","").replace("&lt;","").replace("/p&gt","").replace("/","").replace("&quot;","'")
            except:
                desc = ''
            #print(desc)
            cparent = cat.parent
            #print(cparent)
            subs = cparent.findAll("span",{"class":"rating__score"})

            for sub in subs:
                #print(sub)
                try:
                    score = len(sub.findAll("i", {"class" : "icon-circle-solid active"}))
                    #print(score)
                    CatData = {
                    "name" : NamesMap[cat.text],
                    "score": score,
                    "description": desc,
                    "cat": CatsIDs[score],
                    "votes": None
                    }
                    #print(CatData)
                except:
                    score = None
            Details.append(CatData)

        Review = {
            "id": ID,
            "status": "Sucess",
            "title": title.title(),
            "provider": "CommonSenseMedia",
            "recommended-age": age,
            "review-items": Details,
            "review-link": movie_url,
            "isFamilyFriendly": isFamilyFriendly,
            "review-date": datePublished,
                }

    else:
        print("Problem connecting to provider")
        Review = {
            "id": ID,
            "status": "Failed",
            "title": videoName,
            "provider": "CommonSenseMedia",
            "recommended-age": None,
            "review-items": None,
            "review-link": None,
            "isFamilyFriendly": None,
            "review-date": None,
                }


    return Review