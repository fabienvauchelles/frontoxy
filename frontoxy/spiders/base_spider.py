# -*- coding: utf-8 -*-

from datetime import datetime
from scrapy import Spider, signals
from scrapy.exceptions import CloseSpider
from scrapy.utils.log import TopLevelFormatter

import logging



logger = logging.getLogger(__name__)



class BaseSpider(Spider):

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BaseSpider, cls).from_crawler(crawler, *args, **kwargs)

        crawler.signals.connect(spider._base_spider_opened, signals.spider_opened)

        return spider


    def _base_spider_opened(self, spider):
        # Logging
        filename = u'log/{0}_{1:%Y%m%d_%H%M%S}.log'.format(self.name, datetime.now())

        formatter = logging.Formatter(
            fmt=u'%(asctime)s [%(name)s] %(levelname)s: %(message)s',
            datefmt=u'%Y-%m-%d %H:%M:%S',
        )

        handler = logging.FileHandler(filename)
        handler.addFilter(TopLevelFormatter(['scrapy']))
        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)

        logging.root.addHandler(handler)

        logger.debug(u'[{0}] _base_spider_opened'.format(self.name))


    def parse(self, response):
        self.check_error()


    def check_error(self):
        # Stop spider if error has been raised in pipeline
        if hasattr(self, 'close_error'):
            raise CloseSpider(self.close_error)


    @property
    def scheduler(self):
        return self.crawler.engine.slot.scheduler
