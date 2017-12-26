# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class RabotaUaItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    id = Field()
    vacancy_link = Field()
    vacancy_name = Field()
    company_link = Field()
    company_name = Field()
    vacancy_short_text = Field()
    salary = Field()
    keywords = Field()
    tags = Field()
    creation_date = Field()
    archive_date = Field()

    def SQLInsert(self):
        return """INSERT INTO vacancies
    (id,
     link,
     NAME,
     company_name,
     company_link,
     salary,
     short_text,
     keywords,
     tags,
     archive_date,
     creation_date)
VALUES
    (:id,
     :link,
     :name,
     :company_name,
     :company_link,
     :salary,
     :short_text,
     :keywords,
     :tags,
     :archive_date,
     :creation_date)"""

    def SQLMerge(self):
        return """MERGE INTO vacancies t
USING (SELECT :id id, :keywords keywords, :tags tags FROM dual) s
ON (t.id = s.id)
WHEN MATCHED THEN
    UPDATE
       SET t.keywords     = merge_list(t.keywords, :keywords),
           t.tags         = merge_list(t.tags, :tags),
           t.archive_date = :archive_date,
           t.insert_date  = SYSDATE
WHEN NOT MATCHED THEN
    INSERT
        (id,
         link,
         name,
         company_name,
         company_link,
         salary,
         short_text,
         keywords,
         tags,
         creation_date,
         archive_date,
         insert_date)
    VALUES
        (:id,
         :link,
         :name,
         :company_name,
         :company_link,
         :salary,
         :short_text,
         :keywords,
         :tags,
         :creation_date,
         :archive_date,
         SYSDATE)"""