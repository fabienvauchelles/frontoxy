# -*- coding: utf-8 -*-

from scrapy import Request
from scrapy.http import HtmlResponse

import json
import os
import zipfile



class BlocksReader(object):
    INFO_FORMAT = u'{0}.desc'
    BODY_FORMAT = u'{0}.dat'

    def read(self, source):
        source_filename = os.path.basename(source)

        with zipfile.ZipFile(source) as zf:
            filenames = sorted(set([zipinfo.filename[:10] for zipinfo in zf.infolist()]))
            for filename in filenames:
                source_path = u'{0}/{1}'.format(source_filename, filename)

                # Read info
                desc = zf.read(self.INFO_FORMAT.format(filename))
                info = json.loads(desc)

                url = info['url'].encode('utf8')
                info.pop('url', None)

                headers = info['headers']
                info.pop('headers', None)

                status = info['status']
                info.pop('status', None)

                info_meta = info['meta']
                info_meta['source_path'] = source_path

                # Read content
                content = zf.read(self.BODY_FORMAT.format(filename))
                request = Request(
                    url=url,
                    meta=info_meta
                )

                response = HtmlResponse(
                    url=url,
                    headers=headers,
                    status=status,
                    body=content,
                    request=request,
                )

                yield response
