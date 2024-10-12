import requests
from bs4 import BeautifulSoup
import re


def ParentPreviewsScraper(ID,videoName):
    Session = requests.Session()
    strName = videoName.replace(":", "").replace(" ","-")
    url = 'https://parentpreviews.com/movie-reviews/' + strName
    r = Session.get(url)
    Cats = {
        "A": "None",
        "B": "Mild",
        "C": "Moderate",
        "D": "Severe"
    }
    NamesMap = {
        "Sexual Content" : "Sex & Nudity",
        "Violence": "Violence",
        "Language":"Profanity",
        "Substance Use":"Smoking, Alchohol & Drugs",
        "Sexual Violence":"Sex & Nudity"
    }

    Details, CatData, Reviews, cats = [] ,[], [] ,[]
    Review = {}

    namePattern = re.compile(r'<b>(.*?): ?<\/b>(.*?)[\n]')

    if '200' in str(r):
        Soup = BeautifulSoup(r.text, "html.parser")
        res = Soup.find("a", {"href":"#content-details"})
        blocks = res.findAll("div",{"class":"criteria_row theme_field"})
        DescSoup = Soup#.find("div",{"class":"post_text_area"})
        Desc = re.findall(namePattern,  str(DescSoup))

        for item in Desc:
            Review.update({item[0] : item[1]})

        for block in blocks:
            score = block.find("span", {"class":"criteria_mark theme_accent_bg"}).text.replace("-","").strip()

            # Handle 'plus' scores
            if '+' in score:
                base_score = score[0]
                score_category = Cats.get(base_score, "Unknown")
                if score_category != "Severe":
                    # Move to the next higher category
                    categories = list(Cats.values())
                    next_category_index = categories.index(score_category) + 1
                    score_category = categories[min(next_category_index, len(categories) - 1)]
            else:
                score_category = Cats.get(score, "Unknown")

            try:
                if Review[block.span.text.strip()]:
                    x = Review[block.span.text.strip()]
                else:
                    x = ''
            except:
                x = ''
                pass

            CatData = {
                "name" : NamesMap[block.span.text],
                "score": score,
                "description": x.replace("<p>","").replace("<br/>","").replace("</br>","").replace("</p>","").replace("<b>","").replace("</b>","").replace("<p>","").strip(),
                "cat": score_category,
                "votes": None
            }

            Details.append(CatData)

        Review = {
            "id": ID,
            "status": "Sucess",
            "title": videoName,
            "provider": "ParentPreviews",
            "recommended-age": '',
            "review-items": Details,
            "review-link": url,
        }
    else:
        Review = {
            "id": ID,
            "status": "Failed",
            "title": videoName,
            "provider": "ParentPreviews",
            "recommended-age": '',
            "review-items": None,
            "review-link": None,
        }
    return Review

