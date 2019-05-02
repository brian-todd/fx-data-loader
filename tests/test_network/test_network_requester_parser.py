import unittest

import numpy as np
import pandas as pd

from dateutil import parser
from lzma import LZMAError

from network.parser import FXTickDataParser
from network.requester import FXTickDataRequester

class TestNetworkRequesterParser(unittest.TestCase):
    '''
    Testing fixture validating proper behvior from the request parsing tools.
    '''

    def setUp(self):
        self.currency = 'EURUSD'
        self.start_date = parser.parse('2018-10-01')
        self.data_parser = FXTickDataParser()

        # Preset testing data.
        self.expected_types = [int, int, int, float, float]

        # Request some date for a single hour.
        requester = FXTickDataRequester(self.currency, self.start_date)
        self.resp: bytes = requester.request()

        # Preset sampled testing data.
        self.sample_data: list = [
            (205, 116055, 116052, 1.0, 1.5700000524520874),
            (275, 116057, 116054, 1.0, 1.0),
            (797, 116058, 116053, 2.319999933242798, 5.510000228881836),
            (1070, 116054, 116053, 1.0, 1.0),
            (1264, 116053, 116049, 1.0, 1.7599999904632568),
            (2644, 116052, 116049, 1.5, 1.690000057220459),
            (3150, 116051, 116048, 1.5, 1.3899999856948853),
            (5480, 116051, 116047, 1.5, 5.889999866485596),
            (7738, 116051, 116048, 1.5, 1.7599999904632568),
            (8257, 116052, 116048, 3.369999885559082, 2.140000104904175),
            (8986, 116051, 116049, 1.0, 1.25),
            (11673, 116050, 116048, 1.0, 1.5),
            (12174, 116051, 116046, 2.25, 5.25),
            (13082, 116051, 116046, 2.25, 3.450000047683716),
            (13649, 116051, 116047, 2.440000057220459, 1.0),
            (14157, 116051, 116047, 2.25, 1.0),
            (14516, 116052, 116049, 1.100000023841858, 1.0),
            (15055, 116052, 116048, 1.690000057220459, 1.0),
            (16985, 116052, 116047, 1.0, 1.5700000524520874),
            (17651, 116052, 116047, 1.440000057220459, 6.449999809265137),
            (18700, 116050, 116048, 1.0, 1.1200000047683716),
            (19224, 116051, 116047, 2.25, 2.319999933242798),
            (19753, 116051, 116047, 2.25, 1.2000000476837158),
            (20289, 116051, 116046, 3.069999933242798, 5.320000171661377),
            (21907, 116050, 116047, 1.0, 1.5700000524520874)
        ]

    def test_successful_parsing(self):
        '''
        Validate that the module is able to parse data end to end and produce
        a list of data as an output.
        '''

        # Parse response data.
        parsed: list = self.data_parser.parse(self.resp)
        self.assertIsInstance(parsed, list)

    def test_parsed_types(self):
        '''
        Validate that the module is producing the correct types for each data
        column.
        '''

        # Parse response but consider only the first row.
        parsed: list = self.data_parser.parse(self.resp)[0]
        resp_types: list = [type(entry) for entry in parsed]

        # Validate that the typesets match.
        self.assertTrue(all([i == j for i, j in zip(self.expected_types, resp_types)]))

    def test_correct_response_data(self):
        '''
        Validate that a test request matches local sample data.
        '''

        # Parse the response and take the first 25 elements.
        parsed: list = self.data_parser.parse(self.resp)[:25]

        # Convert to numpy arrays and validate sufficiently close.
        parsed, sample_data = np.array(parsed), np.array(self.sample_data)
        self.assertTrue(np.allclose(parsed, sample_data))

    def test_valid_response_data(self):
        '''
        Validate that there are no invalid data points in our response.
        '''

        # Parse fully data structure.
        parsed: np.ndarray = np.array(self.data_parser.parse(self.resp))

        # Verify that there are no NaNs or negative values.
        self.assertFalse(np.isnan(parsed).any())
        self.assertFalse((parsed < 0).any())

    def test_raise_lzma_error(self):
        '''
        Validate that proper LZMA errors are raised when we pass garbage data.
        '''

        # Through byte stream of random data to validate the correct error is
        # raised.
        with self.assertRaises(LZMAError):
            self.data_parser.parse(b'random')
