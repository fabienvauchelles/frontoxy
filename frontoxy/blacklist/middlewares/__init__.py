# -*- coding: utf-8 -*-


class BlacklistError(Exception):
    def __init__(self, response, message, *args, **kwargs):
        super(BlacklistError, self).__init__(*args, **kwargs)

        self.response = response
        self.message = message


    def __str__(self):
        return self.message
