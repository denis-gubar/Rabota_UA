# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pandas as pd
import datetime
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import os
import logging

class RabotaUaPipeline(object):
    def __init__(self):
        self.items = []
        #self.df  = pd.DataFrame(columns=['company_name', 'vacancy_name', 'vacancy_short_text', 'company_link', 'vacancy_link', 'keywords'])
        #self.df.index.name = 'id'
        pass

    def process_item(self, item, spider):
        found = False
        for i in self.items:
            if i['id'] == item['id']:
                i['keywords'] += item['keywords']
                found = True
                break
        if not found:
            self.items.append(item)
        return item

    def close_spider(self, spider):
        self.df = pd.DataFrame(self.items)
        #self.df = self.df.sort_values(by=['company_name', 'vacancy_name', 'vacancy_link'])
        self.df.to_excel('output/rabota.ua {0}.xlsx'.format(datetime.date.today().strftime(("%Y%m%d"))), sheet_name="Main")
        
        
class DBPipeline(object):
    def __init__(self):
        os.environ["NLS_LANG"] = "RUSSIAN_RUSSIA.AL32UTF8"
        self.engine = create_engine("oracle://sandbox:sandbox@my_vm", encoding='utf-8')
        self.connection = self.engine.connect()

    def process_item(self, item, spider):
        transaction = self.connection.begin()
        self.connection.execute(item.SQLMerge(), id=item['id'],
                                                      link=item['vacancy_link'],
                                                      name=item['vacancy_name'],
                                                      company_name=item['company_name'],
                                                      company_link=item['company_link'],
                                                      salary=item['salary'],
                                                      short_text=item['vacancy_short_text'],
                                                      keywords=str(item['keywords']),
                                                      tags=item['tags'],
                                                      creation_date=item['creation_date'],
                                                      archive_date=item['archive_date'])
        transaction.commit()
        spider.logger.info('Processed {0}[{1}]'.format(item['vacancy_name'], item['vacancy_link']))
        return item

    def close_spider(self, spider):
        self.connection.close()