import scrapy
import mysql.connector
import csv
import socket
import datetime
from scrapy.http import Request
from scrapy.loader.processors import MapCompose, Join
from scrapy.loader import ItemLoader
from trans.items import ContentItem

class TransContent(scrapy.Spider):
    """trans content"""
    name = "trans_content"
    custom_settings = {
    'DOWNLOAD_DELAY' : '85',
    'AUTOTHROTTLE_ENABLED':'True',
    'AUTOTHROTTLE_START_DELAY':'20.0',
    'AUTOTHROTTLE_MAX_DELAY':'360.0',
    'AUTOTHROTTLE_TARGET_CONCURRENCY':'0.25',
    'AUTOTHROTTLE_DEBUG': 'True',
    'CLOSESPIDER_ITEMCOUNT':'10000',
    'HTTPCACHE_ENABLED': 'False',
    'ITEM_PIPELINES':{
        'trans.pipelines.mysql.ContentWriter': 700,
        },
    }
    category = ['economy']

    def start_requests(self):
        """1. access database
            2. fetch links
            3. Yield links for processing"""
        # open database connection and fetch links
        conn = mysql.connector.connect(user='kush', passwd='incorrect', db='crawls', host='localhost', charset="utf8", use_unicode=True)
        cursor = conn.cursor()
        cursor.execute('SELECT link_no, link FROM trans_links where category = %s and processed="False" order by link_no;',(self.category))
        rows = cursor.fetchall()
        conn.close()
        self.logger.info('%s urls fetched', len(rows))


        #iterate through the links
        for link_no,link in rows:
            yield scrapy.Request(link, meta={"link_no": link_no}, headers = {'referer': 'https://www.google.com/' }, callback = self.parse_content)

    def parse_content(self, response):
        loader = ItemLoader(item=ContentItem(), response = response)
        # get the content
        content = response.css("#divDescription::text").extract()
        content = Join()(content)
        content = content.strip()
        if (content == ''):
            content = response.css('.question-statement-complete-styling::text').extract()
            content = Join()(content)
            content = content.strip()
                
        loader.add_value('content', content)
        # get the title 
        loader.add_css('title', '#hTitle::text', MapCompose(str.strip), Join())
            
        #add category
        loader.add_value('category', self.category)
        # add link no
        loader.add_value('link_no', response.meta['link_no'])
        #add housekeeping
        loader.add_value('project', self.settings.get('BOT_NAME'))
        loader.add_value('spider', self.name)
        loader.add_value('server', socket.gethostname())
        loader.add_value('date', datetime.datetime.now())

        yield loader.load_item()

        



            


