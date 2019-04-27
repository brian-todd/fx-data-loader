'''
Basic pipelines that do not include any additional resampling or transformations.

NOTE: Each pipeline is expected to emit a flag at the end indicating success or
      failure.
'''

from typing import Optional

import numpy as np
import pandas as pd

from datetime import datetime

from utils.logger import logger
from network.requester import FXTickDataRequester
from network.parser import FXTickDataParser
from proceesors.ticks import FXTickDataProcessorSQLite, FXTickDataProcessorTabular

LOG = logger()

class FXTickDataBasicPipeline():
    '''
    Basic pipeline to data from API, transform, and load to disk.
    '''

    def __init__(self, currency: str, request_date: datetime):
        self.currency: str = currency
        self.request_date: datetime = request_date

class FXTickDataTabularPipeline(FXTickDataBasicPipeline):
    '''
    Run data pipeline with a tabular processor.
    '''

    def __call__(self, opath: str, sep: Optional[str] = '\t') -> bool:
        '''
        Full data processing pipeline. Emit a flag if the pipeline succeeded.

        :params opath: Path to output directory for writes.
        :params sep: Optionally specify how the data is delimited.
        :returns flag: Boolean flag indicating success of failure.
        '''

        try:
            LOG.info(f'Sending API requests for date {str(self.request_date)} to {opath}')
            data_requester = FXTickDataRequester(self.currency, self.request_date)
            raw_ticks = data_requester.request()

        except Exception as e:
            LOG.error(f'Error requesting data on {self.request_date} for {self.currency}')
            LOG.error(f'Error string: {str(e)}')
            return False

        try:
            LOG.info(f'Parsing API response for date {str(self.request_date)} to {opath}')
            tick_data_parser = FXTickDataParser()
            parsed_ticks = tick_data_parser.parse(raw_ticks)

        except Exception as e:
            LOG.error(f'Error parsing response data on {self.request_date} for {self.currency}')
            LOG.error(f'Error string: {str(e)}')
            return False

        try:
            LOG.info(f'Processing and writing parsed response for date {str(self.request_date)} to {opath}')
            tick_data_processor = FXTickDataProcessorTabular(self.currency, self.request_date)
            processed_tick_data = tick_data_processor.process(parsed_ticks)
            tick_data_processor.write(processed_tick_data, opath)

        except Exception as e:
            LOG.error(f'Error in the process/write stage for date {self.request_date} for {self.currency}')
            LOG.error(f'Error string: {str(e)}')
            return False

        return True

class FXTickDataSQLitePipeline(FXTickDataBasicPipeline):
    '''
    Run data pipeline with a SQLite processor.
    '''

    def __call__(self, db: str, table: str) -> bool:
        '''
        Full data processing pipeline. Emit a flag if the pipeline succeeded.

        :params db: Local DB name.
        :params table: Table where data will be stored.
        :returns flag: Boolean flag indicating success of failure.
        '''

        try:
            LOG.info(f'Sending API requests for date {str(self.request_date)} to {db}.{table}')
            data_requester = FXTickDataRequester(self.currency, self.request_date)
            raw_ticks = data_requester.request()

        except Exception as e:
            LOG.error(f'Error requesting data on {self.request_date} for {self.currency}')
            LOG.error(f'Error string: {str(e)}')
            return False

        try:
            LOG.info(f'Parsing API response for date {str(self.request_date)} to {db}.{table}')
            tick_data_parser = FXTickDataParser()
            parsed_ticks = tick_data_parser.parse(raw_ticks)

        except Exception as e:
            LOG.error(f'Error parsing response data on {self.request_date} for {self.currency}')
            LOG.error(f'Error string: {str(e)}')
            return False

        try:
            LOG.info(f'Processing and writing parsed response for date {str(self.request_date)} to {db}.{table}')
            tick_data_processor = FXTickDataProcessorSQLite(self.currency, self.request_date)
            processed_tick_data = tick_data_processor.process(parsed_ticks)
            tick_data_processor.write(processed_tick_data, db, table)

        except Exception as e:
            LOG.error(f'Error in the process/write stage for date {self.request_date} for {self.currency}')
            LOG.error(f'Error string: {str(e)}')
            return False

        return True
