""" temporal_lib/tlib_timezone.py """

# Standard Library
from datetime import datetime as DateTimeType
from zoneinfo import ZoneInfo

# Third Party
import tzlocal

class TimeZone(ZoneInfo):
	"""
	Custom wrapper for the ZoneInfo class.
	"""
	# NOTE: ZoneInfo is an abstract base class, and itself is a wrapper around tzinfo

	@staticmethod
	def get_local():
		return TimeZone(str(tzlocal.get_localzone()))

	@staticmethod
	def get_utc():
		return TimeZone("UTC")

	def ambiguous_name(self, as_of_datetime=None):
		"""
		WARNING: This particular "name" of a Time Zone is fluid.
		For example, "America/New_York" is EST in the winter and EDT in the summer.
		"""
		if not as_of_datetime:
			# as_of_datetime = DateTimeType.now(UTC)
			as_of_datetime = DateTimeType.now()
		return self._ZoneInfo.tzname(as_of_datetime)  # _PytzShimTimezone.tzname(dt)

	def iana_name(self) -> str:
		# For IANA time zones, calling str() on zoneinfo zone returns the IANA 'key'
		return str(self)

	def localize_datetime(self, naive_datetime) -> DateTimeType:
		"""
		Convert a naive datetime to a localized datetime for this TimeZone.
		"""
		from temporal_lib.core import localize_datetime as _localize_datetime
		return _localize_datetime(naive_datetime, self)


def any_to_timezone(timezone_object) -> TimeZone:

	if isinstance(timezone_object, str):
		return TimeZone(timezone_object)

	if isinstance(timezone_object, ZoneInfo):
		return TimeZone(str(timezone_object))

	if isinstance(timezone_object, TimeZone):
		return timezone_object

	raise TypeError(timezone_object)
