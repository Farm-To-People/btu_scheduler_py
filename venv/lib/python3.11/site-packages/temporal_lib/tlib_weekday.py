""" tlib_weekday.py """

from datetime import timedelta


WEEKDAYS = (
    { 'name_short': 'SUN', 'name_long': 'Sunday' },
    { 'name_short': 'MON', 'name_long': 'Monday' },
    { 'name_short': 'TUE', 'name_long': 'Tuesday' },
    { 'name_short': 'WED', 'name_long': 'Wednesday' },
    { 'name_short': 'THU', 'name_long': 'Thursday' },
    { 'name_short': 'FRI', 'name_long': 'Friday' },
    { 'name_short': 'SAT', 'name_long': 'Saturday' })

WEEKDAYS_SUN0 = (
    { 'pos': 0, 'name_short': 'SUN', 'name_long': 'Sunday' },
    { 'pos': 1, 'name_short': 'MON', 'name_long': 'Monday' },
    { 'pos': 2, 'name_short': 'TUE', 'name_long': 'Tuesday' },
    { 'pos': 3, 'name_short': 'WED', 'name_long': 'Wednesday' },
    { 'pos': 4, 'name_short': 'THU', 'name_long': 'Thursday' },
    { 'pos': 5, 'name_short': 'FRI', 'name_long': 'Friday' },
    { 'pos': 6, 'name_short': 'SAT', 'name_long': 'Saturday' })

WEEKDAYS_MON0 = (
    { 'pos': 0, 'name_short': 'MON', 'name_long': 'Monday' },
    { 'pos': 1, 'name_short': 'TUE', 'name_long': 'Tuesday' },
    { 'pos': 2, 'name_short': 'WED', 'name_long': 'Wednesday' },
    { 'pos': 3, 'name_short': 'THU', 'name_long': 'Thursday' },
    { 'pos': 4, 'name_short': 'FRI', 'name_long': 'Friday' },
    { 'pos': 5, 'name_short': 'SAT', 'name_long': 'Saturday' },
    { 'pos': 6, 'name_short': 'SUN', 'name_long': 'Sunday' })


def next_weekday_after_date(weekday, any_date):
	"""
	Find the next day of week (MON, SUN, etc) after a target date.
	"""
	weekday_int = None
	if isinstance(weekday, int):
		weekday_int = weekday
	elif isinstance(weekday, str):
		weekday_int = weekday_int_from_name(weekday, first_day_of_week='MON')  # Monday-based math below

	days_ahead = weekday_int - any_date.weekday()
	if days_ahead <= 0:  # Target day already happened this week
		days_ahead += 7
	return any_date + timedelta(days_ahead)


def weekday_string_to_shortname(weekday_string):
	"""
	Given a weekday name (MON, Monday, MONDAY), convert it to the short name.
	"""
	if weekday_string.upper() in (day['name_short'] for day in WEEKDAYS):
		return weekday_string.upper()

	ret = next(day['name_short'] for day in WEEKDAYS if day['name_long'].upper() == weekday_string.upper())
	return ret


def weekday_int_from_name(weekday_name, first_day_of_week='SUN'):
	"""
	Return the integer position of a Weekday within a Week.
	"""
	weekday_short_name = weekday_string_to_shortname(weekday_name)
	if first_day_of_week == 'SUN':
		result = next(weekday['pos'] for weekday in WEEKDAYS_SUN0 if weekday['name_short'] == weekday_short_name)
	elif first_day_of_week == 'MON':
		result = next(weekday['pos'] for weekday in WEEKDAYS_MON0 if weekday['name_short'] == weekday_short_name)
	else:
		raise ValueError("Invalid first day of week (expected one of SUN or MON)")
	return result
