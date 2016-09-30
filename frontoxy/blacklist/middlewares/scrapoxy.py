# -*- coding: utf-8 -*-

from frontoxy.blacklist.middlewares import BlacklistError
from frontoxy.frontier.middlewares.schedulers import BaseSchedulerMiddleware
from scrapy.exceptions import IgnoreRequest
from scrapoxy.commander import Commander

import logging
import random
import time



logger = logging.getLogger(__name__)



class BlacklistDownloaderMiddleware(BaseSchedulerMiddleware):

    def __init__(self, crawler):
        super(BlacklistDownloaderMiddleware, self).__init__(crawler)

        self._http_status_codes = crawler.settings.get('BLACKLIST_HTTP_STATUS_CODES', [503])
        self._sleep_min = crawler.settings.get('SCRAPOXY_SLEEP_MIN', 60)
        self._sleep_max = crawler.settings.get('SCRAPOXY_SLEEP_MAX', 180)

        self._commander = Commander(
            crawler.settings.get('API_SCRAPOXY'),
            crawler.settings.get('API_SCRAPOXY_PASSWORD')
        )


    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)


    def process_response(self, request, response, spider):
        try:
            if response.status in self._http_status_codes:
                raise BlacklistError(response, u'HTTP status '.format(response.status))

            return response

        except BlacklistError as ex:
            logger.debug(
                u'Ignoring Blacklisted response %(response)r: %(message)r',
                {'response': response, 'message': ex.message}, extra={'spider': spider},
            )

            self.scheduler.process_exception(request, ex, spider)

            name = response.headers.get(u'x-cache-proxyname')
            self._stop_and_sleep(name)

            raise IgnoreRequest()
        
    
    def _stop_and_sleep(self, name):
        if name:
            alive = self._commander.stop_instance(name)
            if alive < 0:
                logger.error(u'Remove: cannot find instance %s', name)
            elif alive == 0:
                logger.warn(u'Remove: instance removed (no instance remaining)')
            else:
                logger.debug(u'Remove: instance removed (%d instances remaining)', alive)
        else:
            logger.error(u'Cannot find instance name in headers')

        delay = random.randrange(self._sleep_min, self._sleep_max)
        logger.info(u'Sleeping %d seconds', delay)
        time.sleep(delay)
