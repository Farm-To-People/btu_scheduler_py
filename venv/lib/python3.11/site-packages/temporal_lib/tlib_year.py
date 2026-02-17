""" year.py """

# Depency on week.py
# pylint: disable=unused-import

from datetime import (
	date as DateType,
)

from temporal_lib.tlib_types import (
	any_to_date,
	datestr_to_date,
	date_to_iso_string,
	int_to_ordinal_string,
	validate_datatype
)


class Year():

	def __init__(self, year):
		validate_datatype("year", year, int, True)
		if year < 1970:
			raise ValueError("Minimum value for Year is 1970.")
		self.year = year

	@staticmethod
	def _day_of_week_31dec(year):
		"""
		Possible return values are 0 (Sunday) through 6 (Saturday)
		https://en.wikipedia.org/wiki/ISO_week_date
		"""
		result = (year + int(year / 4) - int(year / 100) + int( year /400) ) % 7
		return result

	def number_of_iso_weeks(self):
		# Result will always be 52 or 53.
		# Week 1 of any year is the week that contains the first Thursday in January
		# https://en.wikipedia.org/wiki/ISO_week_date
		weeks = 52
		if Year._day_of_week_31dec(self.year) == 4:
			weeks += 1
		elif Year._day_of_week_31dec(self.year - 1) == 3:
			weeks += 1
		return weeks
