# -*- coding: utf-8 -*-

from collections import deque
from scrapy import Request
from scrapy.responsetypes import Response
from scrapy.core.scheduler import Scheduler
from scrapy.utils.misc import load_object
from queue import FrontierQueue

import logging
import time



logger = logging.getLogger(__name__)



class FrontierException(Exception):

    def __init__(self, message):
        super(FrontierException, self).__init__(message)



class FrontierScheduler(Scheduler):

    @classmethod
    def from_crawler(cls, crawler):
        fqueue = FrontierQueue(crawler)

        return cls(fqueue, crawler)


    def __init__(self, fqueue, crawler):
        self._fqueue = fqueue
        self._crawler = crawler
        self._stats = crawler.stats
        self._pending_requests = deque()

        # Retry
        self._retry_times_max = self._crawler.settings.getint('RETRY_TIMES')

        # Canonical
        canonical_solver_cls = load_object(self._crawler.settings['FRONTIER_CANONICAL_SOLVER'])
        self._canonical_solver = canonical_solver_cls.from_settings(self._crawler.settings)


    def __len__(self):
        return len(self._pending_requests)


    def open(self, spider):
        logger.debug('[FrontierScheduler] open')

        self._fqueue.open(spider)


    def close(self, reason):
        logger.debug('[FrontierScheduler] close')

        self._fqueue.close()


    def has_pending_requests(self):
        return True


    def enqueue_request(self, request):
        # Is redirected ?
        if request.meta.get('redirect_times', 0) > 0:
            logger.debug('[FrontierScheduler] enqueue locally redirected request: %s', request)
            self._pending_requests.append(request)

        else:
            logger.debug('[FrontierScheduler] add request to queue: %s', request)
            self._fqueue.publish_links([request], [])

        return True


    def next_request(self):
        if len(self) > 0:
            request = self._pending_requests.popleft()
            logger.debug('[FrontierScheduler] dequeue next request: %s', request)

        else:
            request = self._fqueue.get_next_request()

            if request:
                logger.debug('[FrontierScheduler] get request from queue: %s', request)

            else:
                logger.debug('[FrontierScheduler] next request queue is empty')


        if request:
            # Add timestamp
            request.meta['request_start'] = self._now_in_ms()

        return request


    def process_spider_output(self, response, result, spider):
        # Find visited urls (actual + canonical)
        urls_other = set([response.url])

        url_canonical = self._canonical_solver.solve(response)
        if url_canonical:
            urls_other.add(url_canonical)

        # Split Request and Response
        requests_new = []
        for element in result:
            if isinstance(element, Request):
                logger.debug('[FrontierScheduler] add request from response to queue: %s', element)
                requests_new.append(element)

            elif isinstance(element, Response):
                logger.debug('[FrontierScheduler] add response to queue: %s', element)

                # Add timestamp
                element.meta['request_end'] = self._now_in_ms()

                self._fqueue.publish_response(element)
                self._stats.inc_value('item_scraped_count', spider=spider)

            else:
                raise FrontierException('Spider can only generate Request or Response object')

        self._fqueue.publish_links(requests_new, list(urls_other))

        return []


    def process_exception(self, request, exception, spider, retry = True):
        retries = request.meta.get('retry_times', 0) + 1

        if retry and retries <= self._retry_times_max:
            request.meta['retry_times'] = retries

            logger.debug('[FrontierScheduler] add request to RETRY queue (%d/%d): %s', request.meta['retry_times'], self._retry_times_max, request)
            self._fqueue.retry_request(request)

        else:
            logger.debug('[FrontierScheduler] add error queue %s: %s', request, str(exception))
            self._fqueue.publish_error(request, exception)


    def _now_in_ms(self):
        return int(round(time.time() * 1000.0))
