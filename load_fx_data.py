'''
Loading script for downloading and parsing FX data.
'''

import argparse
import multiprocessing
import os
import time

from dateutil import parser, utils

from utils import tools
from utils.logger import logger
from pipelines.basic_pipeline import FXTickDataTabularPipeline, FXTickDataSQLiterPipeline

MAX_POLL_TIME: int = 3
MAX_TIMEOUT_TIME: int = 3

LOG = logger()

def main():
    '''
    Load historical data from dukascopy.
    '''

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--pair', help='Specified currency pair', type=str)
    arg_parser.add_argument('--start_date', help='Starting time for data pull', type=str)
    arg_parser.add_argument('--end_date', help='Ending time for data pull', type=str)
    arg_parser.add_argument('--opath', help='Path to dir for ouput writes.', type=str)
    arg_parser.add_argument('--processes', help='Number of processes for data collection', type=int)
    args = arg_parser.parse_args()

    # Check that output directory exists.
    if not os.path.exists(args.opath):
        raise Exception(f'User specified opath does not exist: {args.opath}')

    # Attempt to convert strings to datetime objects.
    start_date, end_date = tools.parse_arg_dates(args.start_date, args.end_date)

    # Parse currency pair for easy logging.
    parsed_pair = tools.parse_currency_pairs(args.pair)
    LOG.info(f'Loading {parsed_pair} data from {str(start_date)} to {str(end_date)}')

    # Each file is stored on an hourly basis.
    # Therefore each iteration must be done on an hourly basis.
    pprocs: list = []
    with multiprocessing.Pool(processes=args.processes) as pool:
        for query_date in tools.valid_date_range(start_date, end_date):
            pipeline = FXTickDataTabularPipeline(args.pair, query_date)
            pprocs.append((pool.apply_async(pipeline, args=(args.opath, ))))

        while not all([proc.ready() for proc in pprocs]):
            ready_procs: list = [proc for proc in pprocs if proc.ready()]
            LOG.info(f'{len(ready_procs)} / {len(pprocs)} total processes finished.')
            LOG.info(f'Waiting {MAX_POLL_TIME}s to poll processes again.')
            time.sleep(MAX_POLL_TIME)

    procs_responses:list =[]
    for proc in pprocs:
        procs_responses.append(proc.get(MAX_TIMEOUT_TIME))

    LOG.info(f'Positive flags emitted: {sum(procs_responses)} / {len(procs_responses)}')
    LOG.info(f'Processed all data for {parsed_pair} from {str(start_date)} to {str(end_date)}')

if __name__ == '__main__':
    main()
