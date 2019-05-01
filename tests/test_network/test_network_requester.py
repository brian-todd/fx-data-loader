import unittest

from datetime import datetime, timedelta
from dateutil import parser
from network.requester import FXTickDataRequester

class TestNetworkRequester(unittest.TestCase):
    '''
    Testing fixture for network requester functionality.
    '''

    def setUp(self):
        '''
        Set curreny pair and date ranges.
        '''

        self.currency = 'EURUSD'
        self.start_date = parser.parse('2019-04-01')

    def test_single_request(self):
        '''
        Validate that the module successfully requests and returns data.
        '''

        requester = FXTickDataRequester(self.currency, self.start_date)
        resp: bytes = requester.request()

        self.assertNotEqual(len(resp), 0)

    def test_multiple_requests(self):
        '''
        Validate that the moduke can make several days worth of requests with
        no issues.
        '''

        responses: list = []
        for date in [self.start_date + timedelta(days=i) for i in range(3)]:
            requester = FXTickDataRequester(self.currency, date)
            responses.append(requester.request())

        self.assertTrue(all([len(resp) > 0 for resp in responses]))

    def test_correct_return_type(self):
        '''
        Validate that the module is always returning some type of bytes object.
        '''

        requester = FXTickDataRequester(self.currency, self.start_date)
        self.assertIsInstance(requester.request(), bytes)

    def test_raise_exception_malformed_date(self):
        '''
        Validate that the module is correctly raising errors for malformed
        input dates.
        '''

        with self.assertRaises(Exception):
            requester = FXTickDataRequester(self.currency, '')
            requester.request()

    def test_raise_exception_malformed_currency_pair(self):
        '''
        Validate that the module is correctly raising errors for malformed
        currency pairs.
        '''

        with self.assertRaises(Exception):
            requester = FXTickDataRequester('', self.start_date)
            requester.request()

    def test_raise_exception_malformed_all_inputs(self):
        '''
        Validate that the module is correctly raising errors for malformed
        currency pairs and malformed dates.
        '''

        with self.assertRaises(Exception):
            requester = FXTickDataRequester('', '')
            requester.request()
