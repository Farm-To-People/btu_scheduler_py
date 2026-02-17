""" calendar.py """

from datetime import date as DateType, timedelta

from temporal_lib.tlib_types import EPOCH_START_YEAR, EPOCH_END_YEAR
from temporal_lib.core import date_range
from temporal_lib.tlib_week import date_to_week_tuple
from temporal_lib.tlib_weekday import WEEKDAYS_SUN0, WEEKDAYS_MON0


class CalendarBuilder():
	"""
	This class is used to build a calendar dataset.
	"""

	def __init__(self,
	             epoch_year=EPOCH_START_YEAR,
	             end_year=EPOCH_END_YEAR,
				 start_of_week='SUN',
				 debug_mode=False):

		self.debug_mode = debug_mode  # Determines if we output additional Error Messages.

		if not isinstance(start_of_week, str):
			raise TypeError("Argument 'start_of_week' should be a Python String.")
		if start_of_week not in ('SUN', 'MON'):
			raise ValueError(f"Argument 'start of week' must be either 'SUN' or 'MON' (value passed was '{start_of_week}'")
		if start_of_week != 'SUN':
			raise NotImplementedError("Temporal cannot currently handle weeks that begin with Monday.")

		# Starting and Ending Year
		if end_year < epoch_year:
			raise ValueError(f"Ending year {end_year} cannot be smaller than Starting year {epoch_year}")
		self.epoch_year = epoch_year
		self.end_year = end_year

		year_range = range(self.epoch_year, self.end_year + 1)  # because Python ranges are not inclusive
		self.years = tuple(year_range)
		self.weekday_names = WEEKDAYS_SUN0 if start_of_week == 'SUN' else WEEKDAYS_MON0
		self.week_dicts = []  # this will get populated as we build.

	@staticmethod
	def build_all(epoch_year=None, end_year=None, start_of_week='SUN'):
		""" Rebuild all Temporal cache key-values. """
		instance = CalendarBuilder(epoch_year=epoch_year,
		                           end_year=end_year,
		                           start_of_week=start_of_week)

		instance.build_weeks()  # must happen first, so we can build years more-easily.
		instance.build_years()
		instance.build_days()

	def build_years(self):
		year_list = []
		for year in self.years:
			year_list.append(self.build_year(year))

	def build_year(self, year):
		""" Create a dictionary of Year metadata and write to Redis. """
		date_start = DateType(year, 1, 1)
		date_end = DateType(year, 12, 31)
		days_in_year = (date_end - date_start).days + 1
		jan_one_dayname = date_start.strftime("%a").upper()
		year_dict = {}
		year_dict['year'] = year
		year_dict['date_start'] = date_start.strftime("%m/%d/%Y")
		year_dict['date_end'] = date_end.strftime("%m/%d/%Y")
		year_dict['days_in_year'] = days_in_year
		# What day of the week is January 1st?
		year_dict['jan_one_dayname'] = jan_one_dayname
		try:
			weekday_short_names = tuple(weekday['name_short'] for weekday in self.weekday_names)
			year_dict['jan_one_weekpos'] = weekday_short_names.index(jan_one_dayname) + 1  # because zero-based indexing
		except ValueError as ex:
			raise ValueError(f"Could not find value '{jan_one_dayname}' in tuple 'self.weekday_names' = {self.weekday_names}") from ex
		# Get the maximum week number (52 or 53)
		max_week_number = max(week['week_number'] for week in self.week_dicts if week['year'] == year)
		year_dict['max_week_number'] = max_week_number

		return year_dict

	def build_days(self):
		start_date = DateType(self.epoch_year, 1, 1)  # could also do self.years[0]
		end_date = DateType(self.end_year, 12, 31)  # could also do self.years[-1]

		days_list = []
		count = 0
		for date_foo in date_range(start_date, end_date):
			day_dict = {}
			day_dict['date'] = date_foo
			day_dict['date_as_string'] = day_dict['date'].strftime("%Y-%m-%d")
			day_dict['weekday_name'] = date_foo.strftime("%A")
			day_dict['weekday_name_short'] = date_foo.strftime("%a")
			day_dict['day_of_month'] = date_foo.strftime("%d")
			day_dict['month_in_year_int'] = date_foo.strftime("%m")
			day_dict['month_in_year_str'] = date_foo.strftime("%B")
			day_dict['year'] = date_foo.year
			day_dict['day_of_year'] = date_foo.strftime("%j")
			# Calculate the week number:
			week_tuple = date_to_week_tuple(date_foo, verbose=False)  # pylint: disable=protected-access
			day_dict['week_year'] = week_tuple[0]
			day_dict['week_number'] = week_tuple[1]
			day_dict['index_in_week'] = int(date_foo.strftime("%w")) + 1  # 1-based indexing
			days_list.append(day_dict)  # Append this dictionary to our List.
			count += 1

		if self.debug_mode:
			print(f"\u2713 Created a list with {count} Temporal Days")
		return days_list

	def build_weeks(self):
		""" Build all the weeks between Epoch Date and End Date """
		# Begin on January 1st
		jan1_date = DateType(self.epoch_year, 1, 1)
		jan1_day_of_week = int(jan1_date.strftime("%w"))  # day of week for January 1st

		week_start_date = jan1_date - timedelta(days=jan1_day_of_week)  # if January 1st is not Sunday, back up.
		week_end_date = None
		week_number = None
		print(f"Temporal is building weeks, starting with {week_start_date}")

		if self.debug_mode:
			print(f"Processing weeks begining with calendar date: {week_start_date}")

		count = 0
		while True:
			# Stop once week_start_date's year exceeds the Maximum Year.
			if week_start_date.year > self.end_year:
				if self.debug_mode:
					print(f"Ending loop on {week_start_date}")
				break

			week_end_date = week_start_date + timedelta(days=6)
			if self.debug_mode:
				print(f"Week's end date = {week_end_date}")
			if (week_start_date.day == 1) and (week_start_date.month == 1):
				# Sunday is January 1st, it's a new year.
				week_number = 1
			elif week_end_date.year > week_start_date.year:
				# January 1st falls somewhere inside the week
				week_number = 1
			else:
				week_number += 1
			tuple_of_dates = tuple(list(date_range(week_start_date, week_end_date)))
			if self.debug_mode:
				print(f"Writing week number {week_number}")
			week_dict = {}
			week_dict['year'] = week_end_date.year
			week_dict['week_number'] = week_number
			week_dict['week_start'] = week_start_date
			week_dict['week_end'] = week_end_date
			week_dict['week_dates'] = tuple_of_dates

			self.week_dicts.append(week_dict)  # internal object in Builder, for use later in build_years

			# Increment to the Next Week
			week_start_date = week_start_date + timedelta(days=7)
			count += 1

		# Loop complete.
		if self.debug_mode:
			print(f"\u2713 Created {count} Temporal Week keys in Redis.")

		return self.week_dicts
