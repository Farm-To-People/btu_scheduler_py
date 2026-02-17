""" date.py """

import calendar
from datetime import (
	date as DateType,
	datetime as DateTimeType,
	timedelta
)

# Third Party
from dateutil.relativedelta import relativedelta
from dateutil.rrule import SU

from temporal_lib.tlib_types import *  # pylint: disable=wildcard-import, unused-wildcard-import
from temporal_lib import tlib_weekday
from temporal_lib.tlib_timezone import TimeZone, any_to_timezone


def date_to_metadata_dict(any_date) -> dict:
	"""
	Build a dictionary that gives all kinds of helpful information about a particular Date.
	"""
	validate_datatype("any_date", any_date, DateType, True)
	return TDate(any_date).as_dict()


class TDate():
	"""
	A wrapper for datetime.date
	"""
	def __init__(self, any_date):
		if not any_date:
			raise TypeError("TDate() : Class argument 'any_date' cannot be None.")
		# To prevent a lot of downstream boilerplate, going to "assume" that strings
		# passed to this class conform to "YYYY-MM-DD" format.
		if isinstance(any_date, str):
			any_date = datestr_to_date(any_date)
		if not isinstance(any_date, DateType):
			raise TypeError("Class argument 'any_date' must be a Python date.")
		self.date = any_date

	def __str__(self):
		return self.as_iso_string()

	def __eq__(self, other):

		if isinstance(other, TDate):
			return self.as_date() == other.as_date()
		raise NotImplementedError(f"Unknown type passed to overloaded __eq__ function: {type(other)}")

	def __add__(self, other):
		# operator overload:

		if isinstance(other, TDate):
			return TDate(self.date + other.date)  # adding two TDates

		if isinstance(other, timedelta):
			return TDate(self.date + other)

		raise NotImplementedError(f"Unknown type passed to overloaded __add__ function: {type(other)}")

	def __sub__(self, other):
		# operator overload: subtracting two TDates
		return self.date - other.date

	def day_of_week_int(self, zero_based=False):
		"""
		Return an integer representing Day of Week (beginning with Sunday)
		"""
		if zero_based:
			return self.date.toordinal() % 7  # Sunday being the 0th day of week
		return (self.date.toordinal() % 7) + 1  # Sunday being the 1st day of week

	def day_of_week_shortname(self):
		return tlib_weekday.WEEKDAYS_SUN0[self.day_of_week_int() - 1]['name_short']

	def day_of_week_longname(self):
		return tlib_weekday.WEEKDAYS_SUN0[self.day_of_week_int() - 1]['name_long']

	def day_of_month(self):
		return self.date.day

	def day_of_month_ordinal(self):
		return int_to_ordinal_string(self.day_of_month())

	def day_of_year(self):
		return int(self.date.strftime("%j"))  # e.g. April 1st is the 109th day in year 2020.

	def month_of_year(self):
		return self.date.month

	def month_of_year_longname(self):
		return self.date.strftime("%B")

	def year(self):
		"""
		Integer representing the calendar date's year.
		"""
		return self.date.year

	def as_date(self):
		return self.date

	def jan1(self):
		return TDate(DateType(year=self.date.year, month=1, day=1))

	def jan1_next_year(self):
		return TDate(DateType(year=self.date.year + 1, month=1, day=1))

	def is_between(self, from_date, to_date):
		return from_date <= self.date <= to_date

	def week_number(self):
		"""
		This function leverages the Redis cache to find the week number.
		"""
		week_tuple = date_to_week_tuple(self.as_date())
		return week_tuple[1]

	def as_iso_string(self):
		return date_to_iso_string(self.date)

	def as_dict(self) -> dict:
		result = {
			"date": self.date,
			"year": self.year(),
			"month_number": self.month_of_year(),
			"month_name": self.month_of_year_longname(),
			"index_in_week": self.day_of_week_int(),
			"week_number": self.week_number(),
			"weekday_name": self.day_of_week_longname()
		}
		return result

	def as_unix_timestamp(self):
		return int(DateTimeType.combine(self.as_date(), DateTimeType.min.time()).timestamp())

	def unixtime_start(self, timezone: object, with_milliseconds: bool=False) -> int:
		"""
		Return 12:00AM midnight as a Unix Timestamp.
		"""
		# NOTE: January 17th 2024: Unlike previous incarnations, this function does not rely on the Host operating system's timezone.
		datetime_start_naive = DateTimeType.combine(self.as_date(), DateTimeType.min.time())
		datetime_start = any_to_timezone(timezone).localize_datetime(datetime_start_naive)
		result = calendar.timegm(datetime_start.utctimetuple())
		if with_milliseconds:
			result = result * 1000
		return result

	def unixtime_end(self, timezone, with_milliseconds=False):
		"""
		Return 11:59:59.999 PM as a Unix Timestamp.
		"""
		# NOTE: January 17th 2024: Unlike previous incarnations, this function does not rely on the Host operating system's timezone.
		datetime_end_naive = DateTimeType.combine(self.as_date() + timedelta(days=1), DateTimeType.min.time())  # Midnight of the Next Day
		datetime_end = any_to_timezone(timezone).localize_datetime(datetime_end_naive)
		result = calendar.timegm(datetime_end.utctimetuple()) - 1  # Subtract 1 integer to get 11:59:59
		if with_milliseconds:
			result = result * 1000
		return result


# NOTE: Although is function is related to weeks, keeping it here to avoid cross-reference problems
def date_to_week_tuple(any_date: DateType, verbose:bool =False) -> tuple:
	"""
	Given a calendar date, return the corresponding week number.
	This uses a special calculation, that prevents "partial weeks"
	"""
	validate_datatype("any_date", any_date, DateType, True)

	any_tdate = TDate(any_date)  # recast as a Temporal TDate
	next_year =  any_tdate.year() + 1
	jan1 = any_tdate.jan1()
	jan1_next = any_tdate.jan1_next_year()

	if verbose:
		print("\n----Verbose Details----")
		print(f"January 1st {any_tdate.year()} is the {int_to_ordinal_string(jan1.day_of_week_int())} day in the week.")
		print(f"January 1st {next_year} is the {int_to_ordinal_string(jan1_next.day_of_week_int())} day in the week.")
		print(f"Day of Week: {any_tdate.day_of_week_longname()} (value of {any_tdate.day_of_week_int()} with 1-based indexing)")
		print(f"{any_tdate.as_iso_string()} Distance from Jan 1st {any_tdate.year()}: {(any_tdate-jan1).days} days")
		print(f"{any_tdate.as_iso_string()} Distance from Jan 1st {next_year}: {(jan1_next-any_tdate).days} days")

	# SCENARIO 1: Function argument is January 1st.
	if (any_tdate.day_of_month() == 1) and (any_tdate.month_of_year() == 1):
		return WeekTuple(year=any_tdate.year(), week_index=1)

	# SCENARIO 2: Member of this year's Week 1, after January 1st.
	if  ( any_tdate.day_of_week_int() > jan1.day_of_week_int() ) and \
		( (any_tdate - jan1).days in range(1, 7)):
		if verbose:
			print("Scenario 2A; calendar date is a member of Week 1 of its year component.")
		return WeekTuple(any_tdate.year(), 1)

	# SCENARIO 3: Member of *next* year's Week 1, in late December before January 1st.
	if  ( any_tdate.day_of_week_int() < jan1_next.day_of_week_int() ) and \
		( (jan1_next - any_tdate).days in range(1, 7)):
		if verbose:
			print("Scenario 2B; target date near beginning of Future Week 1.")
		return WeekTuple(jan1_next.year(), 1)

	# SCENARIO 4: Week number = Find the first Sunday of the year, then modulus 7.
	if verbose:
		print(f"Scenario 3: Target date is not in same Calendar Week as January 1st {any_tdate.year()}/{next_year}")

	first_sundays_date = TDate(jan1.as_date() + relativedelta(weekday=SU))
	first_sundays_day_of_year = first_sundays_date.day_of_year()
	if first_sundays_day_of_year == 1:
		first_full_week = 1
	else:
		first_full_week = 2
	if verbose:
		print(f"Year's first Sunday is {first_sundays_date.as_iso_string()}, with day of year = {first_sundays_day_of_year}")
		print(f"First full week = {first_full_week}")

	# Formula: ( ( Date's Position in Year - Position of First Sunday) / 7 ) + First_Full_Week offset
	delta = int(any_tdate.day_of_year() - first_sundays_day_of_year)
	week_number = int(delta / 7 ) + first_full_week
	return WeekTuple(jan1.year(), week_number)
