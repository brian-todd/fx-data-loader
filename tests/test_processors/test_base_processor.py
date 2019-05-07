import os
import unittest

import numpy as np
import pandas as pd

from dateutil import parser
from datetime import datetime, timedelta

from pandas.util.testing import assert_frame_equal

from network.parser import FXTickDataParser
from network.requester import FXTickDataRequester
from processors.ticks import FXTickDataProcessor

class TestTickDataBaseUtils():
    '''
    Generic utils useful for processor testing.
    '''

    def setUp(self):
        self.currency = 'EURUSD'
        self.start_date = parser.parse('2018-10-01')
        self.requester = FXTickDataRequester(self.currency, self.start_date)
        self.data_parser = FXTickDataParser()

        # Path to testing data.
        self.data_path: str = 'tests/data/'
        self.data_output_path: str = 'tests/data/outs/'

    def _load_data(self, request_date: datetime) -> pd.DataFrame:
        '''
        Load data are parse timestamp columns.

        :params request_date: Datetime for request date used to lookup file.
        :returns local_data: DataFrame containing loaded data.
        '''

        # Determine file name string.
        file_name: str = os.path.join(self.data_path, self.currency + request_date.strftime('%Y%m%dT%H%M%S'))

        # Load data and parse timestamp columns.
        local_data = pd.read_csv(file_name + '.tsv', sep='\t')
        local_data.loc[:, 'ts'] = pd.to_datetime(local_data['ts'])

        return local_data

class TestTickDataBaseProcessor(TestTickDataBaseUtils, unittest.TestCase):
    '''
    Testing fixture validating functionaly of the base processor class.
    '''

    def test_parse_data_multiple_days(self):
        '''
        Validate that the parsed data matches some historical data across
        multiple days.
        '''

        # Iterate through five days worth of request dates.
        for request_date in [self.start_date + timedelta(i) for i in range(5)]:
            requester = FXTickDataRequester(self.currency, request_date)
            raw_ticks: bytes = requester.request()
            parsed: list = self.data_parser.parse(raw_ticks)

            # Process data and validate that it matches on the same date.
            processor = FXTickDataProcessor(self.currency, request_date)
            ticks: pd.DataFrame = processor.process(parsed)

            # Load locally stored tick data.
            local_data = self._load_data(request_date)

            # Validate that the data matches everywhere.
            assert_frame_equal(ticks, local_data)
