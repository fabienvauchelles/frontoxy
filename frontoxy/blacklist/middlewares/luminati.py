# -*- coding: utf-8 -*-

from frontoxy.blacklist.middlewares import BlacklistError
from frontoxy.frontier.middlewares.schedulers import BaseSchedulerMiddleware
from scrapy.exceptions import IgnoreRequest

import base64
import logging
import random



logger = logging.getLogger(__name__)



class BlacklistDownloaderMiddleware(BaseSchedulerMiddleware):

    PROXY_URL = 'http://zproxy.luminati.io:22225'

    def __init__(self, crawler):
        super(BlacklistDownloaderMiddleware, self).__init__(crawler)

        self._http_status_codes = crawler.settings.get('BLACKLIST_HTTP_STATUS_CODES', [503])

        self._login = crawler.settings.get('LUMINATI_LOGIN')
        self._password = crawler.settings.get('LUMINATI_PASSWORD')
        self._zone = crawler.settings.get('LUMINATI_ZONE')

        self._user_agent = crawler.settings.get('USER_AGENT')

        self._counter_max = crawler.settings.get('BACKLIST_MAX_REQUESTS', 10)
        self._reset_session()


    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)


    def process_request(self, request, spider):
        request.meta['proxy'] = self.PROXY_URL
        request.headers['Proxy-Authorization'] = self._proxy_auth
        request.headers['User-Agent'] = self._user_agent


    def process_response(self, request, response, spider):
        try:
            if response.status in self._http_status_codes:
                raise BlacklistError(response, u'HTTP status '.format(response.status))

            self._counter += 1
            if self._counter > self._counter_max:
                logger.debug(u'Max requests: Change IP')
                self._reset_session()

            return response

        except BlacklistError as ex:
            logger.debug(
                u'Ignoring Blacklisted response %(response)r: %(message)r',
                {'response': response, 'message': ex.message}, extra={'spider': spider},
            )

            self._reset_session()
            self.scheduler.process_exception(request, ex, spider)

            raise IgnoreRequest()


    def process_exception(self, request, exception, spider):
        logger.debug(
                u'Ignoring Exception: %(message)r',
                {'message': exception.message}, extra={'spider': spider},
            )

        self._reset_session()
        self.scheduler.process_exception(request, exception, spider)
        raise IgnoreRequest()


    def _reset_session(self):
        session_id = random.random()
        username = u'lum-customer-{0}-zone-{1}-session-{2}'.format(self._login, self._zone, session_id)
        clear_auth = u'{0}:{1}'.format(username, self._password)
        self._proxy_auth = b'Basic {0}'.format(base64.b64encode(clear_auth))
        self._counter = 0
