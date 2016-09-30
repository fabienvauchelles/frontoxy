# -*- coding: utf-8 -*-


class CanonicalSolver(object):

    @classmethod
    def from_settings(cls, settings):
        return cls()


    def __init__(self):
        self.patterns = [
            u'link[rel="canonical"]::attr(href)',
            u'meta[name="twitter:url"]::attr(content)',
            u'meta[property="og:url"]::attr(content)',
            u'meta[name="original-source"]::attr(content)'
        ]


    def solve(self, response):
        for pattern in self.patterns:
            url = response.css(pattern).extract_first()
            if url and len(url) > 0:
                return url

        return



class SimpleCanonicalSolver(CanonicalSolver):

    @classmethod
    def from_settings(cls, settings):
        return cls()


    def __init__(self):
        super(SimpleCanonicalSolver, self).__init__()

        self.patterns = [
            u'link[rel="canonical"]::attr(href)',
        ]
