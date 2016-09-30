# -*- coding: utf-8 -*-

from datetime import datetime

import json
import logging
import random
import time
import zipfile



logger = logging.getLogger(__name__)



class BlocksWriterError(Exception):

    def __init__(self, message):
        super(BlocksWriterError, self).__init__(message)



class BlocksWriter(object):
    INFO_FORMAT = u'{0:0>10d}.desc'
    BODY_FORMAT = u'{0:0>10d}.dat'

    def __init__(self, path, pattern, maxsize, callback=None):
        self._path = path
        self._pattern = pattern
        self._maxfilesize = maxsize
        self._callback = callback

        self._rownum = 0


    def open(self):
        suffix = u'{0:%Y%m%d_%H%M%S}_{1:04}'.format(datetime.now(), random.randint(0, 10000))
        filename = self._pattern.format(suffix)
        self._target = '{0}/{1}'.format(self._path, filename)

        logger.debug(u'[BlocksWriters] create file %s', self._target)

        self._zipfile = zipfile.ZipFile(
            file=self._target,
            mode='w',
            compression=zipfile.ZIP_DEFLATED,
        )
        self._filesize = 0
        self._rownum = 0


    def close(self):
        logger.debug(u'[BlocksWriters] close file %s', self._target)
        self._zipfile.close()

        if self._callback:
            self._callback(self._target)


    def write_responsed(self, responsed):
        logger.debug(u'[BlocksWriters] write %s', responsed['url'])

        now = time.localtime(time.time())

        # Split response to info/body
        body = responsed['body'].encode('utf-8')
        del responsed['body']
        info = json.dumps(responsed)

        # Write info
        zipinfo = zipfile.ZipInfo(
            filename=self.INFO_FORMAT.format(self._rownum),
            date_time=now,
        )
        zipinfo.compress_type = zipfile.ZIP_DEFLATED
        zipinfo.create_system = 0
        self._zipfile.writestr(zipinfo, info)
        self._filesize += zipinfo.compress_size

        # Write body
        zipinfo = zipfile.ZipInfo(
            filename=self.BODY_FORMAT.format(self._rownum),
            date_time=now,
        )
        zipinfo.compress_type = zipfile.ZIP_DEFLATED
        zipinfo.create_system = 0
        self._zipfile.writestr(zipinfo, body)
        self._filesize += zipinfo.compress_size

        self._rownum += 1

        if self._filesize > self._maxfilesize:
            self.close()

            self.open()
