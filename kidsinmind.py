import requests
from bs4 import BeautifulSoup
import re


def KidsInMindScraper(ID, videoName):
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
        "LANGUAGE":"Language",
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
        #resURL = res.find("a")["href"]
        sResults = res.findAll("a")
        print("found " + str(len(sResults)) + " results for " + videoName)
        for sRes in sResults:
            sURLs.append(sRes["href"])
        print(sURLs[0])
        NoRes = re.compile("Nothing matches your search term").findall(str(res))
        print(NoRes)
        if len(NoRes) ==0:
            print("Skipped No Results Phase")
            for k in range(0,len(sURLs)):

                ## Sraping 1st result
                if 'https://kids-in-mind.com' in sURLs[k]:
                    pass
                else:
                    sURLs[k] = 'https://kids-in-mind.com' + sURLs[k]

                if 'https://' in sURLs[k]:
                    pass
                else:
                    sURLs[k] = 'https://' + sURLs[k]

                resURL = sURLs[k]
                print("KidsInMind trying .." + resURL)
                response = Session.get(resURL)
                soup = BeautifulSoup(response.text, "html.parser")

                sPattern3 = r"href.*imdb.*title.(.*?)\/"
                imdbid = str(re.compile(sPattern3).findall(str(soup)))

                if ID in imdbid:
                    print("KidsInMind Found a match in the seach results .." + resURL)
                    try:
                        title = soup.find("div",{"class":"title"}).h1.text.split("|")[0].strip()
                    except:
                        title = videoName


                    ratingstr = soup.title.string#.split("|")[0].split("-")[1].strip().split(".")[0] + "/10"
                    sPattern =  r"(\d)\.(\d)\.(\d)"
                    aMatches = re.compile(sPattern).findall(ratingstr)

                    try:
                        NudeRating = round(int(aMatches[0][0])/2)
                    except:
                        NudeRating = 0

                    #print(title)
                    blocks = soup.findAll("div",{"class":"et_pb_text_inner"})
                    #print(blocks)
                    i=1
                    for block in blocks:
                        if block.p is not None and i <=7:
                            #print("New Block ............")
                            #print(block.p.text)
                            items = block.findAll("h2")
                            if len(items) < 1:
                                items = block.findAll("span")
                            #print(str(items))

                            for item in items:
                                #print("Processing : " + item.text + "from " + str(len(items)))
                                xitem = item.text.replace(title,"").strip()
                                itemtxt = ''.join((x for x in xitem if not x.isdigit())).strip()
                                if itemtxt in AcceptedNames:
                                    #print(xitem)
                                    #print(itemtxt)

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
                                        #print(CatData)
                                        Details.append(CatData)
                            i = i +1
                    #print(Details)

                    Review = {
                        "id": imdbid.replace("['","").replace("']",""),
                        "title": title,
                        "provider": "KidsInMind",
                        "recommended-age": None,
                        "review-items": Details,
                        "review-link": resURL,
                    }
                    break
                else:
                    ## if not the same movie in this search result
                    Review = None
                    k = k + 1
        else:
            Review = {
                "id": ID,
                "status": "Failed",
                "title": videoName,
                "provider": "KidsInMind",
                "recommended-age": None,
                "review-items": None,
                "review-link": None,
                    }
    else:
        Review = {
            "id": ID,
            "status": "Failed",
            "title": videoName,
            "provider": "KidsInMind",
            "recommended-age": None,
            "review-items": None,
            "review-link": None,
                }

    return Review
