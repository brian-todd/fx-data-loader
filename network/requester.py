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

        self.request_date: datetime = request_date
        self.currency: str = currency

        # Validate and parse datetime information.
        self.year, self.month, self.day, self.hour = self._parse_input_date(self.request_date)

        # Parametrized URL for requests.
        self.DUKAS_BASE_URL: str = f'http://www.dukascopy.com/datafeed/{self.currency}/{self.year}/{self.month}/{self.day}/{self.hour}h_ticks.bi5'

    def _parse_input_date(self, request_date: datetime) -> tuple:
        '''
        Parse input date and return a tuple outlining all necessary params.

        :params request_date: Datetime of desired request datetime.
        :returns date_parts: Tuple containing individual date parts.
        '''

        assert isinstance(request_date, datetime), f'Input argument is not datetime: {type(request_date)}'
        try:
            year: str = str(request_date.year)
            month: str = "{0:0>2}".format(request_date.month - 1)
            day: str = "{0:0>2}".format(request_date.day)
            hour: str = "{0:0>2}".format(request_date.hour)

        except Exception as e:
            raise Exception(f'Could not parse input datetime: {request_date}')

        return year, month, day, hour

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
