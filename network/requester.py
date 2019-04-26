'''
Classes used in requesting data from various sources.

NOTE: Each requested file includes one hour of tick data.
'''

import requests

from datetime import datetime

class FXTickDataRequester(object):
    '''
    Request and parse data from Dukascopy FX APIs.
    '''

    def __init__(self, currency: str, request_date: datetime):
        '''
        Build out URL in initializer.

        :params currency: String identifying currency pair.
        :params request_date: Datetime object containing all relevant date parts.
        '''

        self.request_date = request_date

        # Parameters necessary for constructing URL.
        # Convert to string and verify that each parameter has the correct format.
        self.currency: str = currency
        self.year: str = str(request_date.year)
        self.month: str = "{0:0>2}".format(request_date.month - 1)
        self.day: str = "{0:0>2}".format(request_date.day)
        self.hour: str = "{0:0>2}".format(request_date.hour)

        # Parametrized URL for requests.
        self.DUKAS_BASE_URL = f'http://www.dukascopy.com/datafeed/{self.currency}/{self.year}/{self.month}/{self.day}/{self.hour}h_ticks.bi5'

    def request(self) -> bytes:
        '''
        Request tick data per the object parameters set in the constructor.

        :returns resp: Raw reponse containing tick data in bytes.
        '''

        # Request tick data and validate appropriate response code.
        resp = requests.get(self.DUKAS_BASE_URL)
        if not resp.status_code == requests.codes.ok:
            resp.raise_for_status()
            return b''

        return resp.content
