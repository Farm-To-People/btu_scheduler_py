""" __init__.py for module 'cron' """

# Standard L
from datetime import date as DateType, datetime as DateTimeType, timedelta
import re

# Third Party
from cron_converter import Cron	as CronType
import cron_descriptor
from local_crontab import Converter

# temporal-lib
from temporal_lib.tlib_types import (
	any_to_date,
	any_to_time,
	validate_datatype,
	NoneType,
	UTC
)
from temporal_lib.core import get_system_datetime_now, get_utc_datetime_now
from temporal_lib.tlib_timezone import TimeZone


# Type Casting
def datetime_to_cron_string(any_datetime):
	return f"{any_datetime.minute} {any_datetime.hour} {any_datetime.day} {any_datetime.month} * {any_datetime.year}"


def date_and_time_to_cron_string(any_date: DateType, any_time: DateTimeType) -> str:
	"""
	Create a new cron string from any Date and Time combo.
	"""
	validate_datatype("any_date", any_date, DateType, mandatory=True)
	validate_datatype("any_time", any_time, DateTimeType, mandatory=True)

	some_datetime = DateTimeType.combine(any_date, any_time)	# Combine into a single datetime.
	return datetime_to_cron_string(some_datetime)


def time_components_to_cron_string(month, day_of_month, day_of_week, hour, minute):
	"""
	Purpose of this function is to convert individual SQL columns (Hour, Day, Minute, etc.)
	into a valid Unix cron string.

	Input:   A BTU Task Schedule document class.
	Output:   A Unix cron string.
	"""

	cron_day_dictionary = []  # TODO: Figure out what this was in BTU

	datetime_now = get_system_datetime_now()  # Local datetime using System's time zone settings.
	new_datetime = DateTimeType(year=datetime_now.year,
								month=datetime_now.month,
								day=datetime_now.day,
								hour=int(hour) if hour else 0,
								minute=int(minute) if minute else 0,
								second=0, microsecond=0,
								tzinfo=datetime_now.tzinfo)

	utc_datetime = new_datetime.astimezone(UTC)
	cron = [None] * 5

	# Minute of the day
	if isinstance(minute, NoneType):
		cron[0] = "*"
	else:
		cron[0] = str(utc_datetime.minute)

	# Hour of the day
	if not hour:
		cron[1] = "*"
	else:
		cron[1] = str(utc_datetime.hour)

	# Day of the Month
	if not day_of_month:
		cron[2] = "*"
	else:
		str(day_of_month)

	cron[3] = "*" if month is None else month

	if not day_of_week:
		cron[4] = "*"
	else:
		cron[4] = str(cron_day_dictionary[day_of_week[:3]])

	result = " ".join(cron)
	validate_cron_string(result, error_on_invalid=True)
	return result


def validate_cron_string(cron_string: str, error_on_invalid:bool=False) -> bool:
	"""
	Validate that a string is a Unix cron string.
	"""
	# https://www.codeproject.com/Tips/5299523/Regex-for-cron-expressions
	# https://www.codeproject.com/info/cpol10.aspx
	# https://docs.python.org/3/howto/regex.html#non-capturing-and-named-groups

	# minute_component = r"(?P<minute>\*(\/[0-5]?\d)?|[0-5]?\d)"
	minute_component = r"(?P<minute>\*|(?:[0-9]|(?:[1-5][0-9]))(?:(?:\-[0-9]|\-(?:[1-5][0-9]))?|(?:\,(?:[0-9]|(?:[1-5][0-9])))*))"
	hour_component = r"(?P<hour>\*|(?:[0-9]|1[0-9]|2[0-3])(?:(?:\-(?:[0-9]|1[0-9]|2[0-3]))?|(?:\,(?:[0-9]|1[0-9]|2[0-3]))*))"
	day_component = r"(?P<day>\*|(?:[1-9]|(?:[12][0-9])|3[01])(?:(?:\-(?:[1-9]|(?:[12][0-9])|3[01]))?|(?:\,(?:[1-9]|(?:[12][0-9])|3[01]))*))"
	month_component = r"(?P<month>\*|(?:[1-9]|1[012]|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)"
	month_component += r"(?:(?:\-(?:[1-9]|1[012]|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC))?|"
	month_component += r"(?:\,(?:[1-9]|1[012]|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC))*))"
	day_of_week_component = r"(?P<day_of_week>\*|(?:[0-6]|SUN|MON|TUE|WED|THU|FRI|SAT)(?:(?:\-(?:[0-6]|SUN|MON|TUE|WED|THU|FRI|SAT))?|(?:\,(?:[0-6]|SUN|MON|TUE|WED|THU|FRI|SAT))*))"

	crontab_time_format_regex = re.compile(
		rf"{minute_component}\s+{hour_component}\s+{day_component}\s+{month_component}\s+{day_of_week_component}"
	)  # end of re.compile()

	if crontab_time_format_regex.match(cron_string) is None:
		if error_on_invalid:
			raise ValueError(f"String '{cron_string}' is not a valid Unix cron string.")
		return False
	return True

# type: ignore[no-redef]
def local_cron_to_utc_datetimes(cron_string: str,
                                timezone: object,
                                utc_start_datetime=None,
								utc_end_datetime=None,
								number_of_results:int=100):
	"""
	Concept:
		1. Take a local cron, and convert to many UTC crons (because of daylight savings)
		2. For each UTC cron, create a bunch of Future UTC Datetime values.
		3. Limit to a range of DateTime
		4. Limit to certain results (sorted by UTC datetime ascending)
	"""
	validate_cron_string(cron_string, error_on_invalid=True)
	if not utc_start_datetime:
		utc_start_datetime = get_utc_datetime_now()
	if not utc_end_datetime:
		utc_end_datetime = utc_start_datetime + timedelta(days=100)

	# If necessary, convert 'timezone' from a string to TimeZone
	if isinstance(timezone, str):
		timezone: TimeZone = TimeZone(timezone)


	# Use the "local_crontab" library to create a list of UTC crons
	utc_cron_list: list = Converter(cron_string,
	                                timezone.iana_name(),
									year=2023).to_utc_crons()  # List of Strings (not cron objects)
	utc_cron_list = sorted(utc_cron_list)

	known_datetimes = []
	for _, utc_cron_string in enumerate(utc_cron_list):
		schedule = CronType(utc_cron_string).schedule(start_date=utc_start_datetime)  # create a schedule of UTC date times
		for _ in range(100):  # One Year of Dates, Per Schedule
			next_schedule: DateTimeType = schedule.next()
			known_datetimes.append(next_schedule)

	result = sorted(known_datetimes)[0:number_of_results]

	for each in result:
		print(each.isoformat())

	return result


def cron_to_friendly_description(cron_object):
	if isinstance(cron_object, str):
		return cron_descriptor.get_description(cron_object)
	raise TypeError(f"Not sure what to do with cron_string of Type = {type(cron_object)}")
