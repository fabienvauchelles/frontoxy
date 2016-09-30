# -*- coding: utf-8 -*-

from frontoxy.queue import SimpleQueue, RetryQueue
from scrapy.utils.reqser import request_to_dict, request_from_dict
from utils import response_to_dict

import logging



logger = logging.getLogger(__name__)



class FrontierQueue(object):

    def __init__(self, crawler):
        self._crawler = crawler

        # Queues
        queue_url = self._crawler.settings.get('FRONTIER_QUEUE_URL')

        # Queue: Links
        self._queue_links = SimpleQueue(
            queue_url,
            self._crawler.settings.get('FRONTIER_QUEUE_LINKS')
        )

        # Queue: Requests
        self._queue_requests = RetryQueue(
            queue_url,
            self._crawler.settings.get('FRONTIER_QUEUE_REQUESTS'),
            self._crawler.settings.get('FRONTIER_QUEUE_REQUESTS_RETRY_DELAY')
        )

        # Queue: Responses
        self._queue_responses = SimpleQueue(
            queue_url,
            self._crawler.settings.get('FRONTIER_QUEUE_RESPONSES')
        )

        # Queue: Errors
        self._queue_errors = SimpleQueue(
            queue_url,
            self._crawler.settings.get('FRONTIER_QUEUE_ERRORS')
        )


    def open(self, spider):
        self._spider = spider

        self._queue_links.open()
        self._queue_requests.open()
        self._queue_responses.open()
        self._queue_errors.open()


    def close(self):
        self._queue_errors.close()
        self._queue_responses.close()
        self._queue_requests.close()
        self._queue_links.close()


    def get_next_request(self):
        reqd = self._queue_requests.get()
        if not reqd:
            return

        return request_from_dict(reqd, self._spider)


    def publish_links(self, requests_new, urls_other):
        # Serialize requests_new
        reqds_new = []
        for request_new in requests_new:
            try:
                reqd_new = request_to_dict(request_new, self._spider)
                reqds_new.append(reqd_new)
            except ValueError as e:
                logger.error(
                    u'Unable to serialize request: %(request)s - reason: %(reason)s',
                     {'request': request_new, 'reason': e},
                     exc_info=True, extra={'spider': self._spider}
                )

        # Send to queue
        if len(urls_other) <= 0 and len(reqds_new) <= 0:
            return

        payload = {
            'requests_new': reqds_new,
            'urls_other': urls_other,
        }
        self._queue_links.publish(payload)


    def retry_request(self, request):
        try:
            reqd = request_to_dict(request, self._spider)
            self._queue_requests.retry(reqd, reqd['priority'])
        except ValueError as e:
            logger.error(
                u'Unable to serialize request: %(request)s - reason: %(reason)s',
                {'request': request, 'reason': e},
                exc_info=True, extra={'spider': self._spider}
            )


    def publish_response(self, response):
        responsed = response_to_dict(response)
        self._queue_responses.publish(responsed)


    def publish_error(self, request, exception):
        error = {
            'request': request_to_dict(request, self._spider),
            'cls': self._get_exception_class(exception),
            'msg': str(exception)
        }
        self._queue_errors.publish(error)


    def _get_exception_class(self, exception):
        try:
            return exception.__class__.__name__
        except:
            return '?'