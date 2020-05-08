import csv
import scrapy
import datetime
from scrapy.loader.processors import Join, MapCompose
from scrapy.http import Request
from scrapy.loader import ItemLoader
from trans.items import LinkItem

class LinksSpider(scrapy.Spider):
    """ trans links spider"""
    name = "trans_links"
    category = ['psychology']
    start_urls = ['https://www.transtutors.com/questions/humanities/psychology/']

    custom_settings = {
        'DOWNLOAD_DELAY' : '85',
        'AUTOTHROTTLE_ENABLED':'True',
        'AUTOTHROTTLE_START_DELAY':'20.0',
        'AUTOTHROTTLE_MAX_DELAY':'360.0',
        'AUTOTHROTTLE_TARGET_CONCURRENCY':'0.25',
        'AUTOTHROTTLE_DEBUG': 'True',
        'HTTPCACHE_ENABLED': 'True',
        'ITEM_PIPELINES':{
            'trans.pipelines.link_db.LinkSaver': 700,
            },
    }


    def parse(self, response):
        #extract question links
        links = response.css("#hypQuestionUrl::attr(href)").extract()
        #use item loader to save in database
        loader = ItemLoader(item=LinkItem(), response=response)
        loader.add_value('link', links)
        loader.add_value('referring_link', response.url)
        loader.add_value('project', self.settings.get('BOT_NAME'))
        loader.add_value('spider', self.name)
        loader.add_value('date', datetime.datetime.now())
        loader.add_value('category', self.category)

        yield loader.load_item()

        end_page = False

        # if not last page url increment by one
        if response.css("#btnFirst.aspNetDisabled.prev"):
            # we are on the first page
            # concatenate 1
            current_url = response.url + '1/'
        elif response.css("#btnLast.aspNetDisabled.nxt"):
            # we are on last page
            # exit
            end_page = True
        elif response.css("#btnLast"):
            #increment url
            current_url_list = response.url.split('/')
            current_url_list[-2] =str( int(current_url_list[-2])+1)
            current_url = '/'.join(current_url_list)

        if not end_page:
            yield(Request(response.urljoin(current_url), headers = {'referer': 'https://www.google.com/' }, callback = self.parse))

    

        

