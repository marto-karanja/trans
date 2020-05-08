# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.item import Field


class ContentItem(scrapy.Item):
    """content items class"""
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = Field()
    category = Field()
    content = Field()
    link_no = Field()

    #House keeping fields
    url = Field()
    project = Field()
    spider = Field()
    server =  Field()
    date = Field()


class LinkItem(scrapy.Item):
    link = Field()
    referring_link = Field()
    project = Field()
    spider = Field()
    date = Field()
    category = Field()