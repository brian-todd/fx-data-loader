import os
import unittest

import numpy as np
import pandas as pd

from dateutil import parser
from datetime import datetime, timedelta

from pandas.util.testing import assert_frame_equal

from network.parser import FXTickDataParser
from network.requester import FXTickDataRequester
from processors.ticks import FXTickDataProcessorTabular, FXTickDataProcessorSQLite
from tests.test_processors.test_base_processor import TestTickDataBaseUtils

class TestTickDataTabularProcessor(TestTickDataBaseUtils, unittest.TestCase):
    '''
    Testing fixture for tabular data processor.
    '''

    def test_tabular_data_processor(self):
        '''
        Validate that the processor writes tab delimited data.
        '''

        # Iterate through five days worth of request dates.
        for request_date in [self.start_date + timedelta(i) for i in range(5)]:
            requester = FXTickDataRequester(self.currency, request_date)
            raw_ticks: bytes = requester.request()
            parsed: list = self.data_parser.parse(raw_ticks)

            # Process data using tabular parser.
            processor = FXTickDataProcessorTabular(self.currency, request_date)
            ticks: pd.DataFrame = processor.process(parsed)

            # Write sample data using custom writer.
            # If there are any errors writing the data, then fail the test.
            try:
                sample = ticks.head(25).copy()
                processor.write(sample, self.data_output_path)

            except Exception as e:
                self.fail(str(e))


    def test_tabular_data_processor_output(self):
        '''
        Validate that the outputs from prior tests match test data.
        '''

        proc_data_dir: list = os.listdir(self.data_path)
        test_data_dir: list = os.listdir(self.data_output_path)
        for proc_data_path, test_data_path in zip(proc_data_dir, test_data_dir):
            proc_data: pd.DataFrame = pd.read_csv(os.path.join(self.data_path, proc_data_path), sep='\t')
            test_data: pd.DataFrame = pd.read_csv(os.path.join(self.data_output_path, test_data_path), sep='\t')

            # Validate that all data is correct.
            assert_frame_equal(proc_data, test_data)
