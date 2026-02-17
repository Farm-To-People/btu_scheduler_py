""" __init__.py for the temporal package. """

# Standard Library
import datetime
from datetime import date as DateType
from datetime import datetime as DateTimeType
import importlib.metadata
import os
import sys
from zoneinfo import ZoneInfo


class VersionError(Exception):
	def __init__(self, message, errors=None):
		super().__init__(message)
		if errors:
			self.errors = errors
			print(f"Printing Errors:\n{errors}")


if sys.version_info.major != 3:
	raise VersionError("Temporal Library is only available for Python 3.")

__version__ = importlib.metadata.version(__package__ or __name__)

LOCAL_TIMEZONE = None


class ArgumentMissing(Exception):
	pass
class ArgumentType(Exception):
	pass
class NotFoundError(Exception):
	pass


def is_env_var_set(variable_name):
	"""
	Returns true if an Environment Variable is set to 1.
	"""
	if not variable_name:
		return False
	variable_value = os.environ.get(variable_name)
	if not variable_value:
		return False
	try:
		return int(variable_value) == 1
	except Exception:
		return False


class Result():
	"""
	Inspired by Rust's Result type which has Ok(None) or Error(message)
	Functions can return an instance of this class, instead of True/False or None.
	"""
	def __init__(self, success, message, execution_time=None):
		"""
		Arguments:
			success: True/False that the function succeeded.
			message: Text explaining the success message.
			execution_time:  (Optional) How long the function took to complete, in seconds.
		"""
		if not isinstance(success, bool):
			raise TypeError("Result class argument 'success' must be a boolean.")
		if message:
			if not isinstance(message, (str, dict, list)):
				raise TypeError(f"Result class argument 'message' must be a Python String, List, or Dictionary.  Found value '{message}' instead.")
		self.okay = success
		self.message = message or None
		self.execution_time = round(execution_time,2) if execution_time else None

	def __bool__(self):
		"""
		A useful overload.  For example: 'if Result():'
		"""
		return self.okay

	def as_json(self):
		"""
		Dictionary representation of the class instance.
		"""
		return {
		    "okay": self.okay,
		    "message": self.message,
		    "execution_time": self.execution_time
		}

	def as_msgprint(self):
		msg = f"Success: {self.okay}"
		msg += f"<br>Execution Time: {self.execution_time} seconds."
		msg += f"<br><br>Message: {self.message}"
		return msg


def safeset(any_dict, key, value, as_value=False):
	"""
	This function is used for setting values on an existing Object, while respecting current keys.
	"""

	if not hasattr(any_dict, key):
		raise AttributeError(f"Cannot assign value to unknown attribute '{key}' in dictionary {any_dict}.")
	if isinstance(value, list) and not as_value:
		any_dict.__dict__[key] = []
		any_dict.extend(key, value)
	else:
		any_dict.__dict__[key] = value


def dprint(msg, check_env=None, force=None):
	"""
	A print() that only prints when an environment variable is set.
	Very useful for conditional printing, depending on whether you want to debug code, or not.
	"""
	if force:
		print(msg)
	elif is_env_var_set(check_env):
		print(msg)


def set_local_timezone():
	"""
	Necessary to avoid some circular reference problems.
	"""
	from temporal_lib.tlib_timezone import TimeZone
	global LOCAL_TIMEZONE  # pylint: disable=global-statement
	LOCAL_TIMEZONE = TimeZone.get_local()

if not LOCAL_TIMEZONE:
	set_local_timezone()
