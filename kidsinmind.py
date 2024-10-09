import aiohttp
from bs4 import BeautifulSoup
import re
import asyncio

async def KidsInMindScraper(ID, videoName):
    videoName = videoName.replace(":", "%3A").replace(" ","+")
    sURL = f'https://kids-in-mind.com/search-desktop.htm?fwp_keyword={videoName}'
    
    async with aiohttp.ClientSession() as session:
        async with session.get(sURL) as response:
            if response.status != 200:
                return create_error_response(ID, videoName)
            
            html = await response.text()
            soup = BeautifulSoup(html, "html.parser")
            res = soup.find("div", {"class":"facetwp-template"})
            
            if not res:
                return create_error_response(ID, videoName)
            
            sResults = res.findAll("a")
            sURLs = [sRes["href"] for sRes in sResults]
            
            if not sURLs:
                return create_error_response(ID, videoName)
            
            tasks = [process_url(session, url, ID, videoName) for url in sURLs]
            results = await asyncio.gather(*tasks)
            
            for result in results:
                if result and result['status'] == 'Success':
                    return result
            
            return create_error_response(ID, videoName)

async def process_url(session, url, ID, videoName):
    if not url.startswith('https://'):
        url = f'https://kids-in-mind.com{url}' if not url.startswith('http') else url
    
    async with session.get(url) as response:
        if response.status != 200:
            return None
        
        html = await response.text()
        soup = BeautifulSoup(html, "html.parser")
        
        sPattern3 = r"href.*imdb.*title.(.*?)\/"
        imdbid = str(re.compile(sPattern3).findall(str(soup)))
        
        if ID not in imdbid:
            return None
        
        return process_matching_result(soup, ID, videoName, url)

def process_matching_result(soup, ID, videoName, url):
    try:
        title = soup.find("div",{"class":"title"}).h1.text.split("|")[0].strip()
    except:
        title = videoName

    Cats = {
        0: "None", 1: "Clean", 2: "Mild", 3: "Mild", 4: "Mild",
        5: "Moderate", 6: "Moderate", 7: "Moderate",
        8: "Severe", 9: "Severe", 10: "Severe",
    }
    NamesMap = {
        "SEX/NUDITY": "Sex & Nudity",
        "VIOLENCE/GORE": "Violence",
        "LANGUAGE": "Language",
        "SUBSTANCE USE": "Smoking, Alcohol & Drugs",
        "DISCUSSION TOPICS": "Discussion Topics",
        "MESSAGE": "Message",
    }
    AcceptedNames = ['SEX/NUDITY','VIOLENCE/GORE','LANGUAGE','SUBSTANCE USE','DISCUSSION TOPICS','MESSAGE']

    Details = []
    blocks = soup.findAll("div",{"class":"et_pb_text_inner"})
    
    for block in blocks[:7]:
        if block.p is not None:
            items = block.findAll("h2") or block.findAll("span")
            for item in items:
                xitem = item.text.replace(title,"").strip()
                itemtxt = ''.join((x for x in xitem if not x.isdigit())).strip()
                if itemtxt in AcceptedNames:
                    ratetxt = next((int(x) for x in xitem if x.isdigit()), 0)
                    parent = item.parent
                    desc = parent.p.text if parent.p else parent.text
                    CatData = {
                        "name": NamesMap[itemtxt],
                        "score": int(ratetxt)/2,
                        "description": desc,
                        "cat": Cats[ratetxt],
                        "votes": None
                    }
                    Details.append(CatData)

    return {
        "id": ID,
        "status": "Success",
        "title": title,
        "provider": "KidsInMind",
        "recommended-age": None,
        "review-items": Details,
        "review-link": url,
    }

def create_error_response(ID, videoName):
    return {
        "id": ID,
        "status": "Failed",
        "title": videoName,
        "provider": "KidsInMind",
        "recommended-age": None,
        "review-items": None,
        "review-link": None,
    }
