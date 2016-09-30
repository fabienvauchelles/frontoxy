# -*- coding: utf-8 -*-

from scrapy.utils.misc import load_object
from scrapy.utils.reqser import request_to_dict, request_from_dict



def response_to_dict(response):
    return {
        'cls': '{r.__module__}.{r.__class__.__name__}'.format(r=response),
        'encoding': response.encoding,

        'request': request_to_dict(response.request),

        'url': response.url,
        'status': response.status,
        'headers': dict(response.headers),
        'meta': response.meta,

        'body': response.body,
    }



def response_from_dict(responsed):
    respcls = load_object(responsed['cls'])

    request = request_from_dict(responsed['request'])

    response = respcls(
        encoding=responsed['encoding'],
        request=request,
        url=responsed['url'],
        status=responsed['status'],
        headers=responsed['headers'],
        body=responsed['body'],
    )

    response.meta.update(responsed['meta'])

    return response
