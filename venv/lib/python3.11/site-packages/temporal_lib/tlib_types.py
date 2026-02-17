""" types.py """
# This module handles various type conversions.

from __future__ import annotations

from collections import namedtuple
from datetime import date as DateType, datetime as DateTimeType, time as TimeType, timedelta, timezone
import os
import re
from typing import TYPE_CHECKING
import zoneinfo

# Third Party
import dateutil

if TYPE_CHECKING:
	from temporal_lib.tlib_timezone import TimeZone

NoneType = type(None)

# Epoch is the range of 'business active' dates.
EPOCH_START_YEAR = 2020
EPOCH_END_YEAR = 2050
EPOCH_START_DATE = DateType(EPOCH_START_YEAR, 1, 1)
EPOCH_END_DATE = DateType(EPOCH_END_YEAR, 12, 31)

# These should be considered true Min/Max for all other calculations.
MIN_YEAR = 1970
MAX_YEAR = 2201
MIN_DATE = DateType(MIN_YEAR, 1, 1)
MAX_DATE = DateType(MAX_YEAR, 12, 31)

UTC = zoneinfo.ZoneInfo("UTC")

def validate_datatype(argument_name: str, argument_value: object, expected_type: type, mandatory=False):
	"""
	A helpful generic function for checking a variable's datatype, and throwing an error on mismatches.
	Absolutely necessary when dealing with extremely complex Python programs that talk to SQL, HTTP, Redis, etc.

	NOTE: Function is opt-out by default. Set environment variable 'TEMPORAL_TYPE_CHECKING' to opt-in.
	NOTE: expected_type can be a single Type, or a tuple of Types.
	"""

	# Function is opt-out by default to slightly improve the performance.
	if not os.getenv("TEMPORAL_TYPE_CHECKING") in ("1", 1, True):
		return argument_value

	if not isinstance(argument_name, str):
		raise ValueError("Invalid syntax why calling 'validate_datatype'; the first argument must be a String.")

	from temporal_lib import ArgumentMissing, ArgumentType  # Necessary to avoid circular reference issues

	# Throw error if missing mandatory argument.
	if mandatory and isinstance(argument_value, NoneType):
		raise ArgumentMissing(f"Argument '{argument_name}' is mandatory.")

	if isinstance(argument_value, DateType) and argument_value == DateType(1, 1, 1).date():
		raise ArgumentMissing(f"Argument '{argument_name}' is mandatory and cannot be null.")

	if isinstance(argument_value, DateTimeType) and argument_value == DateTimeType(1, 1, 1):
		raise ArgumentMissing(f"Argument '{argument_name}' is mandatory and cannot be null.")

	if not argument_value:
		return argument_value  # datatype is going to be a NoneType, which is okay if not mandatory.

	# Check argument type
	if not isinstance(argument_value, expected_type):
		if isinstance(expected_type, tuple):
			expected_type_names = [ each.__name__ for each in expected_type ]
			msg = f"Argument '{argument_name}' should be one of these types: '{', '.join(expected_type_names)}'"
			msg += f"<br>Found a {type(argument_value).__name__} with value '{argument_value}' instead."
		else:
			msg = f"Argument '{argument_name}' should be of type = '{expected_type.__name__}'"
			msg += f"<br>Found a {type(argument_value).__name__} with value '{argument_value}' instead."
		raise ArgumentType(msg)

	# Otherwise, return the argument to the caller.
	return argument_value


def date_to_iso_string(any_date: DateType) -> str:
	"""
	Given a date, create an ISO String.  For example, 2021-12-26.
	"""
	if not isinstance(any_date, DateType):
		raise TypeError(f"Argument should be of type 'datetime.date', not '{type(any_date)}'")
	return any_date.strftime("%Y-%m-%d")


def datetime_to_iso_string(any_datetime):
	"""
	Given a datetime, create a ISO String
	"""
	if not isinstance(any_datetime, DateTimeType):
		raise TypeError(f"Argument 'any_date' should have type 'datetime', not '{type(any_datetime)}'")

	return any_datetime.isoformat(sep=' ')  # Note: Frappe not using 'T' as a separator, but a space ''


def datetime_to_sql_datetime(any_datetime: DateTimeType):
	"""
	Convert a Python DateTime into a DateTime that can be written to MariaDB/MySQL.
	"""
	return any_datetime.strftime('%Y-%m-%d %H:%M:%S')


# ----------------
# DATES
# ----------------

def is_date_string_valid(date_string):
	# dateutil parser does not agree with dates like "0001-01-01" or "0000-00-00"
	if (not date_string) or (date_string or "").startswith(("0001-01-01", "0000-00-00")):
		return False
	return True

def any_to_date(date_as_unknown):
	"""
	Given an argument of unknown Type, try to return a Date.
	"""
	try:
		if not date_as_unknown:
			return None
		if isinstance(date_as_unknown, str):
			return DateTimeType.strptime(date_as_unknown,"%Y-%m-%d").date()
		if isinstance(date_as_unknown, DateType):
			return date_as_unknown

	except dateutil.parser._parser.ParserError as ex:  # pylint: disable=protected-access
		raise ValueError(f"'{date_as_unknown}' is not a valid date string.") from ex

	raise TypeError(f"Unhandled type ({type(date_as_unknown)}) for argument to function any_to_date()")

def any_to_iso_date_string(any_date):
	"""
	Given a date, create a String that MariaDB understands for queries (YYYY-MM-DD)
	"""
	if isinstance(any_date, DateType):
		return any_date.strftime("%Y-%m-%d")
	if isinstance(any_date, str):
		return any_date
	raise TypeError(f"Argument 'any_date' can be a String or datetime.date only (found '{type(any_date)}')")

def datestr_to_date(date_as_string):
	"""
	Converts an ISO 8601 extended string (YYYY-MM-DD) to datetime.date object.
	"""
	# Don't make assumptions about duck types.
	if not date_as_string:
		return None
	if isinstance(date_as_string, DateType):
		return date_as_string  # was already a Date
	if not isinstance(date_as_string, str):
		raise TypeError(f"Argument 'date_as_string' should be of type String, not '{type(date_as_string)}'")
	if not is_date_string_valid(date_as_string):
		return None

	try:
		# The original function I was using was completely asinine.
		# If you pass a string representing a day of week (e.g. "Friday"), it returns the next Friday in the calendar.  Instead of an error.
		# return dateutil.parser.parse(date_as_string, yearfirst=True, dayfirst=False).date()

		return DateTimeType.strptime(date_as_string,"%Y-%m-%d").date()

	except dateutil.parser._parser.ParserError as ex:  # pylint: disable=protected-access
		raise ValueError("Value '{date_as_string}' is not a valid ISO 8601 extended string.") from ex

# ----------------
# TIMES
# ----------------

def date_to_datetime_midnight(any_date):
	"""
	Return a Date as a Datetime set to midnight.
	"""
	return DateTimeType.combine(any_date, DateTimeType.min.time())


def any_to_time(generic_time):
	"""
	Given an argument of a generic, unknown Type, try to return a Time.
	"""
	try:
		if not generic_time:
			return None
		if isinstance(generic_time, str):
			return timestr_to_time(generic_time)
		if isinstance(generic_time, TimeType):
			return generic_time
		if isinstance(generic_time, timedelta):
			# This is an uncommon conversion, however, I've seen Frameworks use timedelta as a substitute for time.
			minutes, seconds = divmod(generic_time.seconds, 60)
			hours, minutes = divmod(minutes, 60)
			return TimeType(hour=hours, minute=minutes, second=seconds)

	except dateutil.parser._parser.ParserError as ex:  # pylint: disable=protected-access
		raise ValueError(f"'{generic_time}' is not a valid Time string.") from ex

	raise TypeError(f"Function argument 'generic_time' in any_to_time() has an unhandled data type: '{type(generic_time)}'")


def timestr_to_time(time_as_string):
	"""
	Converts a string time (8:30pm) to datetime.time object.
	Examples:
		8pm
		830pm
		830 pm
		8:30pm
		20:30
		8:30 pm
	"""
	time_as_string = time_as_string.lower()
	time_as_string = time_as_string.replace(':', '')
	time_as_string = time_as_string.replace(' ', '')

	am_pm = None
	hour = None
	minute = None
	second = 0

	if 'am' in time_as_string:
		am_pm = 'am'
		time_as_string = time_as_string.replace('am', '')
	elif 'pm' in time_as_string:
		am_pm = 'pm'
		time_as_string = time_as_string.replace('pm', '')
	time_as_string = time_as_string.replace(' ', '')

	# Based on length of string, make some assumptions:
	if len(time_as_string) == 0:
		raise ValueError(f"Invalid time string '{time_as_string}'")
	if len(time_as_string) in (1,2):
		hour = int(time_as_string)
		minute = 0
	elif len(time_as_string) == 3:
		hour = int(time_as_string[0])
		minute = int(time_as_string[1:3])  # NOTE: Python string splicing; last index is not included.
	elif len(time_as_string) == 4:
		hour = int(time_as_string[0:2])  # NOTE: Python string splicing; last index is not included.
		minute = int(time_as_string[2:4]) # NOTE: Python string splicing; last index is not included.
	elif len(time_as_string) == 5:
		# For example 4:13:53 PM
		hour = int(time_as_string[0:1])  # NOTE: Python string splicing; last index is not included.
		minute = int(time_as_string[1:3]) # NOTE: Python string splicing; last index is not included.
		second = int(time_as_string[3:5]) # NOTE: Python string splicing; last index is not included.
	elif len(time_as_string) == 6:
		# For example 16:13:53
		hour = int(time_as_string[0:2])  # NOTE: Python string splicing; last index is not included.
		minute = int(time_as_string[2:4]) # NOTE: Python string splicing; last index is not included.
		second = int(time_as_string[4:6]) # NOTE: Python string splicing; last index is not included.
	else:
		raise ValueError(f"timestr_to_time() : Invalid time string '{time_as_string}'")

	if hour > 23:
		raise ValueError(f"timestr_to_time() : Invalid time string '{time_as_string}'")
	if minute > 59:
		raise ValueError(f"timestr_to_time() : Invalid time string '{time_as_string}'")
	if int(hour) > 12 and am_pm == 'am':
		raise ValueError(f"timestr_to_time() : Invalid time string '{time_as_string}'")

	if not am_pm:
		if hour > 12:
			am_pm = 'pm'
		else:
			am_pm = 'am'
	if am_pm == 'pm' and hour < 12:
		hour += 12

	return TimeType(int(hour), int(minute), int(second))


def timedelta_to_time(any_timedelta):
	"""
	Convert a timedelta to a time, by treating it as simple offset from midnight.
	"""
	return DateTimeType(
		(any_timedelta.seconds // 3600),
		(any_timedelta.seconds % 3600) // 60,
		(any_timedelta.seconds % 60)
	)


class TimeRange():
	def __init__(self, start, end):
		self.start = start
		self.end = end
		self.duration = self.end - self.start

	def is_overlapped(self, time_range):
		if max(self.start, time_range.start) < min(self.end, time_range.end):
			return True
		else:
			return False

	def get_overlapped_range(self, time_range):
		if not self.is_overlapped(time_range):
			return

		if time_range.start >= self.start:
			if self.end >= time_range.end:
				return TimeRange(time_range.start, time_range.end)
			else:
				return TimeRange(time_range.start, self.end)
		elif time_range.start < self.start:
			if time_range.end >= self.end:
				return TimeRange(self.start, self.end)
			else:
				return TimeRange(self.start, time_range.end)

	def __repr__(self):
		# pylint: disable=consider-using-f-string
		return '{0} ------> {1}'.format(*[TimeType.strftime('%Y-%m-%d %H:%M:%S', TimeType.localtime(d))
										  for d in [self.start, self.end]])


# ----------------
# DATETIMES
# ----------------

def any_to_datetime(some_object: object):
	"""
	Given an object of unknown type, try to return a complete DateTime (naive or with time zone)

	Supported string formats:
	    1. %Y-%m-%d %H:%M:%S   (2024-12-25 13:45:09)
	    2.                     (2022-03-10 20:46:37+00:00)
	    3.                     (2022-03-10 20:46:37.554284+00:00)
		4.                     (2021-07-13 17:15:32.541150)
	"""

	if not some_object:
		return None
	if isinstance(some_object, DateTimeType):
		return some_object
	if not isinstance(some_object, str):
		raise TypeError(f"Unhandled type ({type(some_object)}) for argument to function any_to_datetime()")

	some_object = some_object.replace('T', ' ')  # ISO8601 has a 'T' in between the Date and Time components.  For simplicity, replace with an empty space.

	has_milliseconds = has_timezone = False
	if len(some_object) > 19:
		has_milliseconds = '.' in some_object[19:]
		has_timezone = ('+' in some_object[19:]) or ('-' in some_object[19:])

	# Determine which of 4 possible string formats is appropriate:
	if (not has_milliseconds) and (not has_timezone):
		# print(f"Scenario 1: No Milliseconds, No Time Zone ({some_object})")
		datetime_string_format = "%Y-%m-%d %H:%M:%S"
	elif has_milliseconds and has_timezone:
		# print(f"Scenario 2: Both Milliseconds and Time Zone ({some_object})")
		datetime_string_format = "%Y-%m-%d %H:%M:%S.%f%z"
	elif has_milliseconds:
		# print(f"Scenario 3: Milliseconds Only ({some_object})")
		datetime_string_format = "%Y-%m-%d %H:%M:%S.%f"
	elif has_timezone:
		# print(f"Scenario 4: Time Zone Only  ({some_object})")
		datetime_string_format = "%Y-%m-%d %H:%M:%S%z"
	else:
		raise ValueError(f"Unhandled string format: {some_object}")

	try:
		return DateTimeType.strptime(some_object, datetime_string_format)
	except dateutil.parser._parser.ParserError as ex:  # pylint: disable=protected-access
		raise ValueError(f"'{some_object}' is not a valid datetime string.") from ex

	raise TypeError(f"Unhandled type ({type(some_object)}) for argument to function any_to_datetime()")


# ----------------
# TIME ZONES
# ----------------

def any_to_timezone(timezone_object) -> TimeZone:
	from temporal_lib.tlib_timezone import any_to_timezone as _any_to_timezone
	return _any_to_timezone(timezone_object)


def offset_string_to_tzinfo(offset_string):
	# See Stack Overflow article here: https://stackoverflow.com/a/37097784
	match_data = re.match(r"([+\-]?)(\d{2})(\d{2})", offset_string)  # +0530, which is India Standard Time (IST)
	if not match_data:
		match_data = re.match(r"([+\-]?)(\d{2}):(\d{2})", offset_string)  # +05:30, which is India Standard Time (IST)
	if not match_data:
		raise ValueError(f"Unable to determine Time Zone offset from string '{offset_string}'")

	sign, hours, minutes = match_data.groups()
	sign = -1 if sign == '-' else 1
	hours, minutes = int(hours), int(minutes)
	tzinfo = timezone(sign * timedelta(hours=hours, minutes=minutes))
	return tzinfo


# ----------------
# OTHER
# ----------------

def int_to_ordinal_string(some_integer) -> str:
	"""
	Convert an integer into its ordinal representation::
		int_to_ordinal_string(0)   => '0th'
		int_to_ordinal_string(3)   => '3rd'
		int_to_ordinal_string(122) => '122nd'
		int_to_ordinal_string(213) => '213th'
	"""
	# Shamelessly borrowed from here: https://stackoverflow.com/questions/9647202/ordinal-numbers-replacement
	some_integer = int(some_integer)
	if 11 <= (some_integer % 100) <= 13:
		suffix = 'th'
	else:
		suffix = ['th', 'st', 'nd', 'rd', 'th'][min(some_integer % 10, 4)]
	return str(some_integer) + suffix


WeekTuple = namedtuple('WeekTuple', 'year week_index')
