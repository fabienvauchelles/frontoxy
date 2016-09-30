# -*- coding: utf-8 -*-

from frontoxy.blacklist.middlewares import BlacklistError
from frontoxy.frontier.middlewares.schedulers import BaseSchedulerMiddleware
from scrapy.exceptions import IgnoreRequest

import base64
import logging
import random



logger = logging.getLogger(__name__)



class BlacklistDownloaderMiddleware(BaseSchedulerMiddleware):

    def __init__(self, crawler):
        super(BlacklistDownloaderMiddleware, self).__init__(crawler)

        self._http_status_codes = crawler.settings.get('BLACKLIST_HTTP_STATUS_CODES', [503])

        superproxy_country = crawler.settings.get('LUMINATI_SUPERPROXY_COUNTRY', 'uk')
        self._proxy_url = 'http://servercountry-{0}.zproxy.luminati.io:22225'.format(superproxy_country)
        self._login = crawler.settings.get('LUMINATI_LOGIN')
        self._password = crawler.settings.get('LUMINATI_PASSWORD')
        self._zone = crawler.settings.get('LUMINATI_ZONE')
        self._reset_session()

        self._user_agent = crawler.settings.get('USER_AGENT')


    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)


    def process_request(self, request, spider):
        request.meta['proxy'] = self._proxy_url
        request.headers['Proxy-Authorization'] = self._proxy_auth
        request.headers['User-Agent'] = self._user_agent


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

            self._reset_session()
            self.scheduler.process_exception(request, ex, spider)

            raise IgnoreRequest()


    def _reset_session(self):
        session_id = random.random()
        username = u'lum-customer-{0}-zone-{1}-session-{2}'.format(self._login, self._zone, session_id)
        clear_auth = u'{0}:{1}'.format(username, self._password)
        self._proxy_auth = b'Basic {0}'.format(base64.b64encode(clear_auth))
