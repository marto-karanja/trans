import traceback

import dj_database_url
import mysql.connector

from twisted.internet import defer
from twisted.enterprise import adbapi
from scrapy.exceptions import NotConfigured


class ContentWriter(object):
    """
    A spider that writes to MySQL databases

    """

    @classmethod
    def from_crawler(cls, crawler):
        """Retrieves scrapy crawler and accesses pipeline's settings"""

        # Get MySQL URL from settings
        mysql_url = crawler.settings.get('MYSQL_PIPELINE_URL', None)

        # If doesn't exist, disable the pipeline
        if not mysql_url:
            raise NotConfigured

        # Create the class
        return cls(mysql_url)

    def __init__(self, mysql_url):
        """Opens a MySQL connection pool"""

        # Store the url for future reference
        self.mysql_url = mysql_url
        # Report connection error only once
        self.report_connection_error = True

        # Parse MySQL URL and try to initialize a connection
        conn_kwargs = ContentWriter.parse_mysql_url(mysql_url)
        self.dbpool = adbapi.ConnectionPool('mysql.connector', charset='utf8', use_unicode=True, connect_timeout=5, **conn_kwargs)


    def close_spider(self, spider):
        """Discard the database pool on spider close"""
        self.dbpool.close()

    @defer.inlineCallbacks
    def process_item(self, item, spider):
        """Processes the item. Does insert into MySQL"""

        self.logger = spider.logger

        try:
            yield self.dbpool.runInteraction(self.do_insert, item, )
            self.logger.info("[%s]: successfully processed", item["link_no"][0])
        except mysql.connector.Error:
            if self.report_connection_error:
                self.logger.error("Can't connect to MySQL: %s" % self.mysql_url)
                print(traceback.format_exc())
                self.report_connection_error = False
        except:
            print(traceback.format_exc())

        # Return the item for the next stage
        defer.returnValue(item)

    @staticmethod
    def do_insert(tx, item):
        """Insert"""
        sql = """INSERT INTO trans_content (content, title, category, link_no, date_scraped, project, server, spider)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
        link_no = item["link_no"][0]
        

        args = (
            item["content"][0],
            item["title"][0],
            item["category"][0],
            link_no,
            item["date"][0],
            item["project"][0],
            item["server"][0],
            item["spider"][0]
        )
        
        tx.execute(sql, args)
        #update db records
        tx.execute("""UPDATE trans_links set processed = 'True' where link_no = %s""",(link_no,))


    @staticmethod
    def parse_mysql_url(mysql_url):
        """
        Parses mysql url and prepares arguments for
        adbapi.ConnectionPool()
        """

        params = dj_database_url.parse(mysql_url)

        conn_kwargs = {}
        conn_kwargs['host'] = params['HOST']
        conn_kwargs['user'] = params['USER']
        conn_kwargs['passwd'] = params['PASSWORD']
        conn_kwargs['db'] = params['NAME']
        conn_kwargs['port'] = params['PORT']

        # Remove items with empty values
        conn_kwargs = dict((k, v) for k, v in conn_kwargs.items() if v)
        
        return conn_kwargs