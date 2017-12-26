import scrapy
import re
import datetime
from urllib.parse import quote
from Rabota_ua.items import RabotaUaItem
import os
import errno

url_region_id = 1 #Kiev
url_parent_id = 1 #IT
#url_period = 3 #7 days
url_period = 1 #30 days
url_last_date = (datetime.datetime.now() - datetime.timedelta(7)).strftime("%d.%m.%Y")

def create_url(key_word, page):
    if key_word == '':
        return "https://rabota.ua/jobsearch/vacancy_list?regionId={0}&parentId={1}&period={2}&lastdate={3}&pg={4}".format(
            quote(str(url_region_id)),
            quote(str(url_parent_id)),
            quote(str(url_period)),
            quote(str(url_last_date)),
            quote(str(page))
        )
    else:
        return "https://rabota.ua/jobsearch/vacancy_list?regionId={0}&keyWords={1}&period={2}&lastdate={3}&pg={4}".format(
            quote(str(url_region_id)),
            quote(str(key_word)),
            quote(str(url_period)),
            quote(str(url_last_date)),
            quote(str(page))
        )

#key_words = ['Oracle', 'ETL', 'BI', 'SQL']
key_words = ['']
url_list = []
for key_word in key_words:
    url_list.append(create_url(key_word, 1))

class RabotaUASpider(scrapy.Spider):
    name = "rabota.ua"
    start_urls = url_list

    def parse(self, response):
        searchObj = re.search('&keyWords=(\w+)', response.url)
        if searchObj:
            keyword = searchObj.group(1)
        else:
            keyword = ''
        page = '1'
        searchObj = re.search('&pg=(\d+)', response.url)
        if searchObj:
            page = searchObj.group(1)
        filename = 'output/rabota-ua-{0}-{1}.html'.format(keyword, page)
        with open(filename, 'wb') as f:
            f.write(response.body)
        table = response.css('table[class="f-vacancylist-tablewrap"]')
        for tr in table.css('tr[id]'):
            vacancy_id = tr.css('tr::attr(id)').extract_first()
            item = RabotaUaItem()
            item['vacancy_link'] = tr.css('a[class="f-visited-enable ga_listing"]::attr(href)').extract_first()
            vacancy_name = tr.css('a[class="f-visited-enable ga_listing"]::text').extract_first()
            if vacancy_name:
                item['vacancy_name'] = vacancy_name.strip()
            else:
                item['vacancy_name'] = ''
            item['company_link'] = tr.css('a[class="f-text-dark-bluegray f-visited-enable"]::attr(href)').extract_first()
            company_name = tr.css('a[class="f-text-dark-bluegray f-visited-enable"]::text').extract_first()
            if company_name:
                item['company_name'] = company_name.strip()
            else:
                item['company_name'] = ''
            vacancy_short_text = tr.css('p[class="f-vacancylist-shortdescr f-text-gray fd-craftsmen"]::text').extract_first()
            if vacancy_short_text:
                item['vacancy_short_text'] = vacancy_short_text.strip()
            item['keywords'] = [keyword]
            salary = tr.css('p[class="fd-beefy-soldier -price"]::text').extract_first()
            if salary:
                item['salary'] = salary.strip()
            else:
                item['salary'] = ''
            item['id'] = vacancy_id
            next_request = scrapy.Request(response.urljoin(item['vacancy_link']), callback=self.parse_vacancy_link)
            next_request.meta['item'] = item
            yield next_request
        next_url = table.css('a[id="content_vacancyList_gridList_linkNext"]::attr(href)').extract_first()
        if next_url:
            yield scrapy.Request(response.urljoin(next_url))

    def parse_vacancy_link(self, response):
        item = response.meta['item']
        filename = 'output{0}.html'.format(item['vacancy_link'])
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        with open(filename, 'wb') as f:
            f.write(response.body)
        item['creation_date'] = datetime.datetime.strptime(response.css('meta[property="article:published_time"]::attr(content)').extract_first().replace(" GMT+2", ""), "%Y-%m-%d %H:%M:%S")
        archive_date = response.css('title::text').re_first(r'(?<=В архиве с )\d{2}\.\d{2}\.\d{4}')
        if archive_date:
            item['archive_date'] = datetime.datetime.strptime(archive_date, "%d.%m.%Y")
        else:
            item['archive_date'] = ''
        item['tags'] = response.css('script::text').re_first(r'(?<=var keywords =)\[[^\]]*\](?=;)')
        if item['tags'] is None:
            item['tags'] = ''
        item['tags'] = item['tags'].replace('"', "'").replace("','", "',  '")
        yield item