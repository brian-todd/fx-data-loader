'''
Process and write tick level data from parsed API response data.
'''

import os
import sqlite3

import numpy as np
import pandas as pd

from datetime import datetime


class FXTickDataProcessor():
    '''
    Base class for tick data processing.
    '''

    def __init__(self, currency: str, request_date: datetime):
        '''
        Set scaling factor for the data returned from the API.
        '''

        self.currency: str = currency
        self.request_date: datetime = request_date

        # This map provides a lookup of known values.
        self.CURRENCY_FACTOR_MAP: dict = {
            'GBPUSD'    : 1e5,
            'EURUSD'    : 1e5
        }

    def process(self, daily_tick_data: list) -> pd.DataFrame:
        '''
        Process tick level data into DataFrame. This is the standard method for
        processing the output data, but can be overwritten by any other classes.

        :params daily_tick_data: Raw tick data in a nested iterable.
        :returns data: Post-processed tick data as a DataFrame.
        '''

        # Convert data into Pandas DataFrame.
        data: pd.DataFrame = pd.DataFrame(daily_tick_data, columns=['ms', 'ask', 'bid', 'ask_volume', 'bid_volume'])

        # Process timestamp data.
        data['ts'] = self.request_date
        data['ts'] = data['ts'] + pd.to_timedelta(data['ms'], unit='ms')
        data.drop('ms', axis=1, inplace=True)

        # Process pip level data.
        data['ask'] = data['ask'] / self.CURRENCY_FACTOR_MAP[self.currency]
        data['bid'] = data['bid'] / self.CURRENCY_FACTOR_MAP[self.currency]

        # Reorder columns.
        return data[['ts', 'ask', 'bid', 'ask_volume', 'bid_volume']]

class FXTickDataProcessorTabular(FXTickDataProcessor):
    '''
    Write data after processing into delimited, tabular format.
    '''

    def write(self, data: pd.DataFrame, opath: str, sep='\t') -> None:
        '''
        For the given output path, write data in a delimited format.

        :params data: DataFrame containing processed tick data.
        :params opath: Output path for writes.
        :params sep: Delimiter between each item.
        '''

        outs: str = os.path.join(opath, self.currency + self.request_date.strftime('%Y%m%dT%H%M%S'))
        data.to_csv(outs + '.tsv', sep=sep, index=False)

class FXTickDataProcessorSQLite(FXTickDataProcessor):
    '''
    Write data after processing into a SQLite database.

    NOTE: This presupposes the existing of local SQLite DB.
    '''

    def write(self, data: pd.DataFrame, db: str, table: str) -> None:
        '''
        Write data into specified DB and table.

        :params data: DataFrame containing processed tick data.
        :params db: Local DB name.
        :params table: Table where data will be stored.
        '''

        # Attempt DB connection. If there are no errors that must be handled,
        # then append data to existing table.
        # At the end of the block close the connection if it is still open.
        try:
            conn = sqlite3.connect(db)

        except sqlite3.Errors as e:
            raise e

        else:
            data.to_sql(table, conn, if_exists='append')

        finally:
            # Check if connection is still open. If so, then close it before
            # releasing our object.
            if conn:
                conn.close()
