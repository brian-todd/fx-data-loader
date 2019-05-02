import unittest

from datetime import datetime
from dateutil import utils

import utils.tools as tools

class TestUtilityToolsFunctions(unittest.TestCase):
    '''
    Test fixture for utility and tooling functions.

    NOTE: These tests may be broken up into multiple modules in the future as
          utility functions expand.
    '''

    def setUp(self):
        # Data used in date parsing utilities.
        self.start_date_string: str = '2018-10-01'
        self.end_date_string: str = '2018-10-07'
        self.start_date: datetime = datetime.strptime(self.start_date_string, '%Y-%m-%d')
        self.end_date: datetime = datetime.strptime(self.end_date_string, '%Y-%m-%d')

        # Data used in currency parsing utilities.
        self.currency= 'EURUSD'

    def test_date_parsing_start_end_dates(self):
        '''
        Validate that the function correctly parses a combination of start and
        end dates.
        '''

        # Parse dates and verify they produce the correct datetime object.
        parsed_start, parsed_end = tools.parse_arg_dates(self.start_date_string, self.end_date_string)

        self.assertEqual(self.start_date, parsed_start)
        self.assertEqual(self.end_date, parsed_end)

    def test_date_parsing_start_no_end_dates(self):
        '''
        Validate that the function correctly parses dates when there is no end
        date supplied.
        '''

        parsed_start, parsed_end = tools.parse_arg_dates(self.start_date_string)

        self.assertEqual(self.start_date, parsed_start)
        self.assertEqual(utils.today(), parsed_end)

    def test_valid_date_ranges_length(self):
        '''
        Validate that the function produces the correct number of days.
        '''

        # Produce collection of dates for a single week.
        dates: set = set()
        for date in tools.valid_date_range(self.start_date, self.end_date):
            dates.add(date.weekday())

        # We expect to get FIVE dates, since we ignore Saturdays and the date
        # endpoints are not inclusive.
        self.assertEqual(len(dates), 5)

    def test_valid_date_ranges_hours(self):
        '''
        Validate that the function produces the expected number of hours per day.
        '''

        # Produce collection of dates for a single day.
        custom_end_date: datetime = datetime.strptime('2018-10-02', '%Y-%m-%d')
        for counter, hour in enumerate(tools.valid_date_range(self.start_date, custom_end_date)):
            pass

        # We expect to have 23 total "hours". This is because we start at the
        # 0th hour and iterate upward.
        self.assertEqual(counter, 23)

    def test_currency_parser(self):
        '''
        Validate the function correctly parses currency pairs.
        '''

        parsed: str = tools.parse_currency_pairs(self.currency)
        self.assertEqual('EUR/USD', parsed)
