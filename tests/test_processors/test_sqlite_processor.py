import os
import subprocess
import unittest

import sqlite3

import numpy as np
import pandas as pd

from datetime import timedelta
from pathlib import Path

from pandas.util.testing import assert_frame_equal

from network.parser import FXTickDataParser
from network.requester import FXTickDataRequester
from processors.ticks import FXTickDataProcessorSQLite
from tests.test_processors.test_base_processor import TestTickDataBaseUtils

class TestTickDataSQLiteProcessor(TestTickDataBaseUtils, unittest.TestCase):
    '''
    Testing fixture for SQLite processing scripts.
    '''

    def _make_test_database(self):
        '''
        Create a portable testing database.
        '''

        # Delete existing DB first.
        self._delete_test_database()

        # Create DB file and run make table script.
        Path(self.path_to_test_db).touch()
        os.system(f'sqlite3 {self.path_to_test_db} < utils/sql/queries/make_tables.sql')

    def _delete_test_database(self):
        '''
        Remove existing testing database.
        '''

        if os.path.exists(self.path_to_test_db):
            os.remove(self.path_to_test_db)

    def _query_data(self, sql_date) -> pd.DataFrame:
        '''
        Query data for the specified date.
        '''

        with sqlite3.connect(self.path_to_test_db) as conn:
            cursor = conn.cursor()
            results = cursor.execute(
                f'''
                SELECT
                    *
                FROM raw_ticks
                WHERE strftime('%Y%m%d', ts) = '{sql_date}'
                ''')

            data = pd.DataFrame(results, columns=['ts', 'pair', 'ask', 'bid', 'ask_volume', 'bid_volume'])
            data['ts'] = pd.to_datetime(data['ts'])
            data['ask'] = pd.to_numeric(data['ask'])
            data['bid'] = pd.to_numeric(data['bid'])
            data['ask_volume'] = pd.to_numeric(data['ask_volume'])
            data['bid_volume'] = pd.to_numeric(data['bid_volume'])

            return data[['ts', 'ask', 'bid', 'ask_volume', 'bid_volume']]

    def test_sqlite_data_processor(self):
        '''
        Validate that the proessor is writing to a database.
        '''

        self._make_test_database()

        # Iterate through five days worth of request dates.
        for request_date in [self.start_date + timedelta(i) for i in range(5)]:
            requester = FXTickDataRequester(self.currency, request_date)
            raw_ticks: bytes = requester.request()
            parsed: list = self.data_parser.parse(raw_ticks)

            # Process data using tabular parser.
            processor = FXTickDataProcessorSQLite(self.currency, request_date)
            ticks: pd.DataFrame = processor.process(parsed)

            # Write sample data using custom writer.
            # If there are any errors writing the data, then fail the test.
            try:
                processor.write(ticks, self.path_to_test_db, 'raw_ticks')

            except Exception as e:
                self.fail(str(e))

    def test_sqlite_data_processor_columns(self):
        '''
        Validate that the correct columns are in the processed data.
        '''

        correct_columns: list = ['ts', 'pair', 'ask', 'bid', 'ask_volume', 'bid_volume']

        # Iterate through five days worth of request dates.
        for request_date in [self.start_date + timedelta(i) for i in range(5)]:
            requester = FXTickDataRequester(self.currency, request_date)
            raw_ticks: bytes = requester.request()
            parsed: list = self.data_parser.parse(raw_ticks)

            # Process data using tabular parser.
            processor = FXTickDataProcessorSQLite(self.currency, request_date)
            ticks: pd.DataFrame = processor.process(parsed)
            ticks_processed: pd.DataFrame = processor._add_currency_pair_column(ticks)

            # Validate that the correct columns are present in the correct order.
            self.assertEqual(list(ticks_processed.columns), correct_columns)

    def test_sqlite_data_processor_output(self):
        '''
        Validate that the outputs from the prior test match test data.]
        '''

        # Iterate through each test data file and validate that their data
        # matches what is in the DB.
        proc_data_dir = [file for file in os.listdir(self.data_path) if 'tsv' in file]
        for proc_data_path in proc_data_dir:
            try:
                proc_data: pd.DataFrame = pd.read_csv(os.path.join(self.data_path, proc_data_path), sep='\t')
                proc_data['ts'] = pd.to_datetime(proc_data['ts'])
                proc_data['ask'] = pd.to_numeric(proc_data['ask'])
                proc_data['bid'] = pd.to_numeric(proc_data['bid'])
                proc_data['ask_volume'] = pd.to_numeric(proc_data['ask_volume'])
                proc_data['bid_volume'] = pd.to_numeric(proc_data['bid_volume'])

            except Exception as e:
                print(os.path.join(self.data_path, proc_data_path))
                raise e

            # Determine dates for each file.
            sql_date = proc_data_path.split('T')[0][-8:]
            data = self._query_data(sql_date)

            # Validate that all data is correct.
            assert_frame_equal(proc_data, data)

        # Cleanup the testing DB.
        self._delete_test_database()