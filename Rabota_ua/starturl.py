import datetime
from urllib.parse import quote

url_region_id = 1 #Kiev
url_period = 3 #7 days
url_last_date = (datetime.datetime.now() - datetime.timedelta(7)).strftime("%d.%m.%Y")

def create_url(key_word, page):
    return "https://rabota.ua/jobsearch/vacancy_list?regionId={0}&keyWords={1}&period={2}&lastdate={3}&pg={4}".format(
        quote(str(url_region_id)),
        quote(str(key_word)),
        quote(str(url_period)),
        quote(str(url_last_date)),
        quote(str(page))
    )

key_words = ['Oracle', 'ETL', 'BI', 'SQL']
url_list = []
for key_word in key_words:
    url_list.append(create_url(key_word, 1))

print(url_list)
