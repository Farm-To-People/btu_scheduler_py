""" week.py """

from datetime import date as DateType
from datetime import timedelta

# Third Party
from dateutil.rrule import SA
from dateutil.relativedelta import relativedelta

# Temporal
from temporal_lib import NotFoundError, tlib_year
from temporal_lib.tlib_types import (
	any_to_date,
	datestr_to_date,
	date_to_iso_string,
	validate_datatype,
	WeekTuple,
	MIN_YEAR, MAX_YEAR
)

from temporal_lib.core import date_range
from temporal_lib.tlib_date import date_to_week_tuple



class Week():
	""" A calendar week, starting on Sunday, where the week containing January 1st is always week #1 """
	def __init__(self, week_tuple: tuple):

		validate_datatype("week_tuple", week_tuple, (tuple, WeekTuple), mandatory=True)
		metadata = get_metadata_from_week_tuple(week_tuple)
		self.week_year = metadata["year"]
		self.week_number = metadata["week_number"]
		self.week_number_str = str(self.week_number).zfill(2)
		self.week_key = str(self.week_year) + self.week_number_str
		self.dates: list = list(metadata["dates_in_week"])
		self.date_start = self.dates[0]
		self.date_end = self.dates[6]

	@staticmethod
	def date_to_week(any_date: DateType) -> 'Week':
		"""
		Given a datetime date, returns an instance of the 'Week' class.
		"""
		# https://mail.python.org/archives/list/python-dev@python.org/thread/CLVXXPQ2T2LQ5MP2Y53VVQFCXYWQJHKZ/
		validate_datatype("any_date", any_date, DateType, True)
		return Week(date_to_week_tuple(any_date))

	def list_of_day_strings(self):
		"""
		Returns self.days as a List of ISO Date Strings.
		"""
		return [ date_to_iso_string(each_date) for each_date in self.dates ]

	def as_tuple(self):
		return (self.week_year, self.week_number)

	def print(self):
		message = f"Week Number: {self.week_number}"
		message += f"\nYear: {self.week_year}"
		message += f"\nWeek Number (String): {self.week_number_str}"
		message += f"""\nDays: {", ".join(self.list_of_day_strings())}"""
		message += f"\nStart: {self.date_start}"
		message += f"\nEnd: {self.date_end}"
		print(message)


def get_weeks_as_dict(from_year: int, from_week_num: int, to_year: int, to_week_num: int):
	""" Given a range of Week numbers, return a List of dictionaries.

		From Shell: bench execute --args "2021,15,2021,20" temporal.get_weeks_as_dict
	"""
	# Convert JS strings into integers.
	from_year = int(from_year)
	from_week_num = int(from_week_num)
	to_year = int(to_year)
	to_week_num = int(to_week_num)

	# From
	if from_year not in range(MIN_YEAR, MAX_YEAR):
		raise ValueError(f"Invalid value '{from_year}' for argument 'from_year'")
	if from_week_num not in range(1, 54):  # 53 possible week numbers.
		raise ValueError(f"Invalid value '{from_week_num}' for argument 'from_week_num'")
	# To
	if to_year not in range(MIN_YEAR, MAX_YEAR):
		raise ValueError(f"Invalid value '{from_year}' for argument 'to_year'")
	if to_week_num not in range(1, 54):  # 53 possible week numbers.
		raise ValueError(f"Invalid value '{to_week_num}' for argument 'to_week_num'")

	weeks_list = get_week_tuples_in_range(from_week_tuple=(from_year, from_week_num),
										  to_week_tuple=(to_year, to_week_num)
	)

	result = []
	for each_tuple in weeks_list:
		validate_datatype("each_tuple", each_tuple, tuple, True)
		week_dict = get_metadata_from_week_tuple(each_tuple)
		result.append({
			"week_start": week_dict["dates_in_week"][0],
			"week_end": week_dict["dates_in_week"][6],
			"week_number": week_dict["week_number"],
			"year":  week_dict["year"],
			"week_dates": week_dict["dates_in_week"]
		})
	return result


def get_week_tuples_in_range(from_week_tuple, to_week_tuple) -> list:
	"""
	Given a starting and ending week tuple (year, week_number), return a List of all week tuples in that range.
	"""
	from_date = _week_tuple_to_first_date_in_week(from_week_tuple)
	to_date = _week_tuple_to_last_date_in_week(to_week_tuple)

	list_of_week = list(week_generator(from_date, to_date))
	return [ each.as_tuple() for each in list_of_week ]  # convert list of Week to list of tuple (year, week number)


def week_generator(from_date, to_date):
	"""
	Return a Python Generator containing all Week class instances for a date range.
	"""
	from_date = any_to_date(from_date)
	to_date = any_to_date(to_date)

	if from_date > to_date:
		raise ValueError("Argument 'from_date' cannot be greater than argument 'to_date'")
	# If dates are the same, simply return the 1 week.
	if from_date == to_date:
		yield Week.date_to_week(from_date)

	from_week = Week.date_to_week(from_date)  # Class of type 'Week'
	if not from_week:
		raise NotFoundError(f"Unable to find a Week for date {from_date}. (Temporal week_generator() and Cache)")
	to_week = Week.date_to_week(to_date)  # Class of type 'Week'
	if not to_week:
		raise NotFoundError(f"Unable to find a Week for date {to_date} (Temporal week_generator() and Cache)")

	# Determine which Week Numbers are missing.
	for year in range(from_week.week_year, to_week.week_year + 1):

		start_index = from_week.week_number if year == from_week.week_year else 1

		# End Index
		end_index = 0
		if year == to_week.week_year:
			end_index = to_week.week_number
		else:
			end_index = tlib_year.Year(year).number_of_iso_weeks()

		for week_num in range(start_index, end_index + 1):
			yield Week((year, week_num))  # A class of type 'Week'


def datestr_to_week_tuple(date_as_string):
	"""
	Given a string date, return the Week Number.
	"""
	return date_to_week_tuple(datestr_to_date(date_as_string), verbose=False)


def _week_tuple_to_last_date_in_week(week_tuple: tuple):
	"""
	Returns the the last calendar date for any week.  Always a Saturday for the Western-2 calendar.
	"""
	validate_datatype("week_tuple", week_tuple, (tuple, WeekTuple), True)

	year = week_tuple[0]
	week_number = week_tuple[1]
	jan1: DateType = DateType(year, 1, 1)

	# Last date of a week is always Saturday.
	first_saturday_of_year = jan1 + relativedelta(weekday=SA)
	offset_days = (week_number-1) * 7
	last_date_in_week: DateType = first_saturday_of_year + timedelta(days=offset_days)

	return last_date_in_week


def _week_tuple_to_first_date_in_week(week_tuple: tuple):
	"""	
	Return the first calendar date of a week tuple (year, week_number)
	"""
	return _week_tuple_to_last_date_in_week(week_tuple) - timedelta(days=6)


def get_metadata_from_week_tuple(week_tuple: WeekTuple) -> dict:
	"""
	Returns an instance of the Week class
	"""
	# https://mail.python.org/archives/list/python-dev@python.org/thread/CLVXXPQ2T2LQ5MP2Y53VVQFCXYWQJHKZ/
	validate_datatype("week_tuple", week_tuple, (tuple, WeekTuple), True)

	last_date_in_week = _week_tuple_to_last_date_in_week(week_tuple)
	first_date_in_week = last_date_in_week - timedelta(days=6)
	dates_in_week: list = list(date_range(first_date_in_week, last_date_in_week))
	week_dict = {
		"year": week_tuple[0],
		"week_number": week_tuple[1],
		"dates_in_week": dates_in_week,
		}
	return week_dict
