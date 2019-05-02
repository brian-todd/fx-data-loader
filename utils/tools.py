'''
Basic toolbox of helper functions.
'''

from typing import Generator, Optional

from datetime import datetime
from dateutil import parser, utils
from dateutil.rrule import HOURLY, rrule, MO, TU, WE, TH, FR, SU

def parse_arg_dates(start_date: str, end_date: Optional[str] = None) -> tuple:
    '''
    Parse strings and return a tuple of start and end dates.

    :params start_date: String representing the start_date from the command line.
    :params end_date: String representing the end_date from the command line.
    :returns dates: Tuple of start and end datetimes.
    '''

    # Attempt to convert strings to datetime objects.
    start_date: datetime = parser.parse(start_date)
    end_date: datetime = parser.parse(end_date) if end_date is not None else utils.today()

    return start_date, end_date

def valid_date_range(start_date: datetime, end_date: datetime) -> Generator[datetime, None, None]:
    '''
    Generate a range of valid business dates based on start and end time.

    NOTE: This range is not inclusive of the final date.

    :params start_date: Starting time of date range.
    :params end_date: Ending time of the date range.
    :returns date_range: List of datetimes across the specified range.
    '''

    for date in rrule(HOURLY, dtstart=start_date, until=end_date, byweekday=(MO,TU,WE,TH,FR,SU)):
        if date == end_date:
            break

        # Try to avoid making requests when the markets close on Friday.
        # On account of DST, the market close and open may shift slightly on
        # Saturday and Sunday, respectively. We'll try to minimize unnecessary
        # requests during that time.
        if date.weekday() == 4 and date.hour > 21:
            continue

        if date.weekday() == 6 and date.hour < 21:
            continue

        yield date

def parse_currency_pairs(pair: str) -> str:
    '''
    Parse an input string to a LOG friendly string.

    :params pair: Currency pair to be split into log friendly string.
    :returns parsed_pair: Parsed loggable output pair format.
    '''

    # Validate that a valid data type is passed.
    if not isinstance(pair, str):
        raise Exception(f'Currency pairs must be expressed as type str, not {type(pair)}: {pair}')

    # Validate this is a legitimate pair.
    if len(pair) != 6:
        raise Exception(f'This is not a valid length currency pair: {pair}')

    return pair[:3] + '/' + pair[3:]
