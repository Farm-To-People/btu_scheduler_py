""" __init__.py for module 'core' """

# Module Typing: https://docs.python.org/3.8/library/typing.html#module-typing


from datetime import (
	date as DateType,
	datetime as DateTimeType,
	timedelta,
)
from typing import Generator

# Temporal - Only import specific functions and constants; avoid circular references.
from temporal_lib.tlib_types import (
	any_to_date,
	datestr_to_date,
	validate_datatype,
	UTC, EPOCH_START_DATE, EPOCH_END_DATE
)

# --------
# Current System Time
# --------

def get_system_datetime_now(time_zone=None) -> DateTimeType:
	"""
	Return the current DateTime in the system's local Time Zone.
	"""
	from temporal_lib.tlib_timezone import any_to_timezone, TimeZone

	time_zone: TimeZone = any_to_timezone(time_zone) if time_zone else TimeZone.get_local()
	utc_datetime = DateTimeType.now(UTC)
	return utc_datetime.astimezone(time_zone)  # convert UTC to local zone


def get_system_date(time_zone=None) -> DateType:
	return get_system_datetime_now(time_zone).date()


# --------
# UTC
# --------

def get_utc_datetime_now() -> DateTimeType:
	return DateTimeType.now(UTC)


def is_datetime_naive(any_datetime) -> bool:
	"""
	Returns True if the datetime is missing a Time Zone component.
	"""
	if not isinstance(any_datetime, DateTimeType):
		raise TypeError("Argument 'any_datetime' must be a Python datetime object.")

	if any_datetime.tzinfo is None:
		return True
	return False


def make_datetime_naive(any_datetime) -> DateTimeType:
	"""
	Takes a timezone-aware datetime, and makes it naive.
	"""
	return any_datetime.replace(tzinfo=None)


def localize_datetime(datetime_object, timezone_object):
	"""
	Given a naive datetime and time zone, return the localized datetime.

	Necessary because Python is -extremely- confusing when it comes to datetime + timezone.
	"""
	from temporal_lib.tlib_timezone import any_to_timezone
	validate_datatype("datetime_object", datetime_object, DateTimeType, True)

	if datetime_object.tzinfo:
		raise TypeError(f"Datetime value {datetime_object} is already localized and time zone aware (tzinfo={datetime_object.tzinfo})")

	# WARNING: DO NOT USE syntax `naive_datetime.astimezone(timezone)`.  That would implicitly shift the UTC offset.
	return datetime_object.replace(tzinfo=any_to_timezone(timezone_object))


def date_is_between(any_date, start_date, end_date, use_epochs=True):
	"""
	Returns a boolean if a date is between 2 other dates.
	The interesting part is the epoch date substitution.
	"""
	if (not use_epochs) and (not start_date):
		raise ValueError("Function 'date_is_between' cannot resolve Start Date = None, without 'use_epochs' argument.")
	if (not use_epochs) and (not end_date):
		raise ValueError("Function 'date_is_between' cannot resolve End Date = None, without 'use_epochs' argument.")

	if not start_date:
		start_date = EPOCH_START_DATE
	if not end_date:
		end_date = EPOCH_END_DATE

	any_date = any_to_date(any_date)
	start_date = any_to_date(start_date)
	end_date = any_to_date(end_date)

	return bool(start_date <= any_date <= end_date)


def date_range(start_date, end_date) -> Generator:
	"""
	Generator for an inclusive range of dates.
	It's very weird this isn't part of Python Standard Library or datetime  :/
	"""

	# Convert from Strings to Dates, if necessary.
	start_date = any_to_date(start_date)
	end_date = any_to_date(end_date)
	# Important to add +1, otherwise the range is -not- inclusive.
	for number_of_days in range(int((end_date - start_date).days) + 1):
		yield start_date + timedelta(number_of_days)


def date_range_from_strdates(start_date_str, end_date_str):
	""" Generator for an inclusive range of date-strings. """
	if not isinstance(start_date_str, str):
		raise TypeError("Argument 'start_date_str' must be a Python string.")
	if not isinstance(end_date_str, str):
		raise TypeError("Argument 'end_date_str' must be a Python string.")
	start_date = datestr_to_date(start_date_str)
	end_date = datestr_to_date(end_date_str)
	return date_range(start_date, end_date)


def date_ranges_to_dates(date_ranges: list) -> set:
	"""
	Generator for multiple, inclusive ranges of dates.
	It's very weird this isn't part of Python Standard Library or datetime  :/

	args:
		date_ranges: List of Tuples, for example: [ (2023-10-01, 2023-10-19) , (2023-11-15, 2023-11-30), (2023-12-09, 2023-12-13)]
	"""

	validate_datatype("date_ranges", date_ranges, (set, list))
	if not date_ranges:
		return set()

	result = set()
	for each_tuple in date_ranges:
		start_date = any_to_date(each_tuple[0]) or any_to_date("1900-01-01")
		end_date = any_to_date(each_tuple[1]) or any_to_date("2199-12-31")

		# Interestingly, Python will not allow the following 2 statements to be combined; it's a syntax error
		temp_results = list(date_range(start_date, end_date))
		if temp_results:
			result.update(temp_results)

	return sorted(result)


def date_generator_type_1(start_date, increments_of, earliest_result_date):
	"""
	Given a start date, increment N number of days.
	First result can be no earlier than 'earliest_result_date'
	"""
	iterations = 0
	next_date = start_date
	while True:
		iterations += 1
		if (iterations == 1) and (start_date == earliest_result_date):  # On First Iteration, if dates match, yield Start Date.
			yield start_date
		else:
			next_date = next_date + timedelta(days=increments_of)
			if next_date >= earliest_result_date:
				yield next_date


def calc_future_dates(epoch_date, multiple_of_days, earliest_result_date, qty_of_result_dates):
	"""
		Purpose: Predict future dates, based on an epoch date and multiple.
		Returns: A List of Dates

		Arguments
		epoch_date:           The date from which the calculation begins.
		multiple_of_days:     In every iteration, how many days do we move forward?
		no_earlier_than:      What is earliest result date we want to see?
		qty_of_result_dates:  How many qualifying dates should this function return?
	"""
	validate_datatype('epoch_date', epoch_date, DateType, True)
	validate_datatype('earliest_result_date', earliest_result_date, DateType, True)

	# Convert to dates, always.
	epoch_date = any_to_date(epoch_date)
	earliest_result_date = any_to_date(earliest_result_date)
	# Validate the remaining data types.
	validate_datatype("multiple_of_days", multiple_of_days, int)
	validate_datatype("qty_of_result_dates", qty_of_result_dates, int)

	if earliest_result_date < epoch_date:
		raise ValueError(f"Earliest_result_date '{earliest_result_date}' cannot precede the epoch date ({epoch_date})")

	this_generator = date_generator_type_1(epoch_date, multiple_of_days, earliest_result_date)
	ret = []
	for _ in range(qty_of_result_dates):  # underscore because we don't actually need the index.
		ret.append(next(this_generator))
	return ret


def get_earliest_date(list_of_dates) -> DateType:
	if not all(isinstance(x, DateType) for x in list_of_dates):
		raise ValueError("All values in argument must be datetime dates.")
	return min(list_of_dates)


def get_latest_date(list_of_dates) -> DateType:
	if not all(isinstance(x, DateType) for x in list_of_dates):
		raise ValueError("All values in argument must be datetime dates.")
	return max(list_of_dates)


def make_ordinal(some_integer) -> str:
	"""
	Convert an integer into its ordinal representation::
		make_ordinal(0)   => '0th'
		make_ordinal(3)   => '3rd'
		make_ordinal(122) => '122nd'
		make_ordinal(213) => '213th'
	"""
	# Shamelessly borrowed from here: https://stackoverflow.com/questions/9647202/ordinal-numbers-replacement
	some_integer = int(some_integer)
	if 11 <= (some_integer % 100) <= 13:
		suffix = 'th'
	else:
		suffix = ['th', 'st', 'nd', 'rd', 'th'][min(some_integer % 10, 4)]
	return str(some_integer) + suffix
