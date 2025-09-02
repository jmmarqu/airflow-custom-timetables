"""
Custom Airflow Timetables for Advanced Scheduling

This module defines a collection of custom Airflow Timetable classes for flexible DAG scheduling patterns,
including monthly, quarterly, yearly, business day, and interval-based schedules.

Available classes include:
- MonthlyLastDay, MonthlyOnDay, MonthlyMultipleDays, MonthlyWeekdayOccurrence, MonthlyLastDayExceptWeekend
- QuarterlyFirstDay, QuarterlyLastDay
- YearlyFirstDay, YearlyWeekdayOccurrence
- WeeklyOnDay, BiweeklyOnDay, SemiMonthly, EveryNDays, EveryNInterval
- BusinessDayOfMonth, CronTimetable

Each class can be used as the `timetable` argument in your DAG definition for advanced scheduling needs.
"""

from airflow.timetables.base import DataInterval, TimeRestriction, DagRunInfo, Timetable
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.plugins_manager import AirflowPlugin
from pendulum import DateTime, timezone as pendulum_timezone, duration, datetime , now
import pendulum
from typing import Optional


class MonthlyLastDay(Timetable):
    """
    Timetable for scheduling a DAG to run on the last day of each month at a specific hour, minute, and second.

    **Parameters:**
    - `hour` (`int`): Hour of the day (0-23). Default is 0.
    - `minute` (`int`): Minute of the hour (0-59). Default is 0.
    - `second` (`int`): Second of the minute (0-59). Default is 0.
    - `tz` (`str`): Timezone string (default "America/New_York").
    
    **Examples:**
    Last day of every month at midnight:
        MonthlyLastDay()
    Last day of every month at 18:30:
        MonthlyLastDay(hour=18, minute=30)
    """

    def __init__(self, hour: int = 0, minute: int = 0, tz: str = "America/New_York", second: int = 0):
        self.tz = tz
        self.hour = hour
        self.minute = minute
        self.second = second
        self.description = f" Monthly, Last day of the Month at {self.hour:02d}:{self.minute:02d} ({self.tz})"

    def serialize(self):
        return {
            "tz": self.tz,
            "hour": self.hour,
            "minute": self.minute,
            "second": self.second,
        }

    @classmethod
    def deserialize(cls, data):
        return cls(
            tz=data.get("tz", "America/New_York"),
            hour=data.get("hour", 0),
            minute=data.get("minute", 0),
            second=data.get("second", 0),
        )

    def _get_last_day(self, year: int, month: int):
        tz = pendulum_timezone(self.tz)
        if month == 12:
            next_month = datetime(year + 1, 1, 1, tz=tz)
        else:
            next_month = datetime(year, month + 1, 1, tz=tz)
        last_day = next_month.subtract(days=1)
        return last_day.replace(hour=self.hour, minute=self.minute, second=self.second, microsecond=0)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        year = run_after.year
        month = run_after.month
        last_day = self._get_last_day(year, month)
        if run_after < last_day:
            # Go to previous month
            if month == 1:
                year -= 1
                month = 12
            else:
                month -= 1
            last_day = self._get_last_day(year, month)
        start = last_day
        end = start.add(hours=1)
        return DataInterval(start=start, end=end)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction
    ) -> DagRunInfo | None:
        tz = pendulum_timezone(self.tz)
        # Establish a baseline: when catchup is disabled, we schedule from "now" forward.
        baseline = restriction.earliest or pendulum.now(tz)
        current_time = pendulum.now(tz)
        if not restriction.catchup:
            baseline = max(baseline, current_time)

        if last_automated_data_interval is None:
            after = baseline
        else:
            after = last_automated_data_interval.end
            if not restriction.catchup and after < current_time:
                after = current_time

        year = after.year
        month = after.month

        # Find the next last day of the month >= after
        while True:
            last_day = self._get_last_day(year, month)
            if last_day >= after:
                break
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1

        start = last_day
        end = start.add(hours=1)

        if restriction.latest is not None and start > restriction.latest:
            return None

        return DagRunInfo.interval(start, end)

class MonthlyOnDay(Timetable):
    """
    Timetable for scheduling a DAG to run on a specific day of each month at a specific hour/minute.

    **Parameters:**
    - `day` (`int`): Day of the month (1-31)
    - `hour` (`int`): Hour of the day (0-23)
    - `minute` (`int`): Minute of the hour (0-59)
    - `second` (`int`): Second of the minute (default 0)
    - `tz` (`str`): Timezone string (default "America/New_York")
    """
    def __init__(self, day: int = 1, hour: int = 0, minute: int = 0, tz: str = "America/New_York", second: int = 0):
        self.day = day
        self.tz = tz
        self.hour = hour
        self.minute = minute
        self.second = second
        self.description = f" Monthly, Day {self.day} at {self.hour:02d}:{self.minute:02d} ({self.tz})"

    def serialize(self):
        return {
            "day": self.day,
            "tz": self.tz,
            "hour": self.hour,
            "minute": self.minute,
            "second": self.second,
        }

    @classmethod
    def deserialize(cls, data):
        return cls(
            day=data.get("day", 1),
            tz=data.get("tz", "America/New_York"),
            hour=data.get("hour", 0),
            minute=data.get("minute", 0),
            second=data.get("second", 0),
        )

    def _get_run_date(self, year: int, month: int):
        tz = pendulum_timezone(self.tz)
        # Clamp day to last day of month if needed
        last_day = (datetime(year, month + 1, 1, tz=tz) - pendulum.duration(days=1)).day if month < 12 else (datetime(year + 1, 1, 1, tz=tz) - pendulum.duration(days=1)).day
        day = min(self.day, last_day)
        return datetime(year, month, day, self.hour, self.minute, self.second, tz=tz)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        year = run_after.year
        month = run_after.month
        run_date = self._get_run_date(year, month)
        if run_after < run_date:
            # Go to previous month
            if month == 1:
                year -= 1
                month = 12
            else:
                month -= 1
            run_date = self._get_run_date(year, month)
        start = run_date
        end = start.add(hours=1)
        return DataInterval(start=start, end=end)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction
    ) -> DagRunInfo | None:
        tz = pendulum_timezone(self.tz)
        # Establish a baseline: when catchup is disabled, schedule from "now" forward.
        baseline = restriction.earliest or pendulum.now(tz)
        current_time = pendulum.now(tz)
        if not restriction.catchup:
            baseline = max(baseline, current_time)

        if last_automated_data_interval is None:
            after = baseline
        else:
            after = last_automated_data_interval.end
            if not restriction.catchup and after < current_time:
                after = current_time

        year = after.year
        month = after.month

        # Find the next run date >= after
        while True:
            run_date = self._get_run_date(year, month)
            if run_date >= after:
                break
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1

        start = run_date
        end = start.add(hours=1)

        if restriction.latest is not None and start > restriction.latest:
            return None

        return DagRunInfo.interval(start, end)

class MonthlyMultipleDays(Timetable):
    """
    Timetable for scheduling a DAG to run on multiple specific days of each month at a specific hour/minute.

    **Parameters:**
    - `days` (`list[int]`): List of days of the month (e.g., [1, 15, 28])
    - `hour` (`int`): Hour of the day (0-23)
    - `minute` (`int`): Minute of the hour (0-59)
    - `second` (`int`): Second of the minute (default 0)
    - `tz` (`str`): Timezone string (default "America/New_York")

    **Examples:**
    1st and 15th of every month at 08:00:
        MonthlyMultipleDays(days=[1, 15], hour=8, minute=0)
    """

    def __init__(self, days=None, hour: int = 0, minute: int = 0, tz: str = "America/New_York", second: int = 0):
        if days is None:
            days = [1]
        self.days = sorted(set(days))
        self.tz = tz
        self.hour = hour
        self.minute = minute
        self.second = second
        self.description = (
            f" Monthly, Days {self.days} at {self.hour:02d}:{self.minute:02d} ({self.tz})"
        )

    def serialize(self):
        return {
            "days": self.days,
            "tz": self.tz,
            "hour": self.hour,
            "minute": self.minute,
            "second": self.second,
        }

    @classmethod
    def deserialize(cls, data):
        return cls(
            days=data.get("days", [1]),
            tz=data.get("tz", "America/New_York"),
            hour=data.get("hour", 0),
            minute=data.get("minute", 0),
            second=data.get("second", 0),
        )

    def _get_next_run_date(self, after):
        tz = pendulum_timezone(self.tz)
        year = after.year
        month = after.month
        # Get last day of the month
        if month == 12:
            last_day = (datetime(year + 1, 1, 1, tz=tz) - pendulum.duration(days=1)).day
        else:
            last_day = (datetime(year, month + 1, 1, tz=tz) - pendulum.duration(days=1)).day
        # Only consider valid days in this month
        valid_days = [d for d in self.days if 1 <= d <= last_day]
        # Find the next valid day in this month
        for d in valid_days:
            candidate = datetime(year, month, d, self.hour, self.minute, self.second, tz=tz)
            if candidate >= after:
                return candidate
        # If not found, go to next month and repeat
        while True:
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1
            if month == 12:
                last_day = (datetime(year + 1, 1, 1, tz=tz) - pendulum.duration(days=1)).day
            else:
                last_day = (datetime(year, month + 1, 1, tz=tz) - pendulum.duration(days=1)).day
            valid_days = [d for d in self.days if 1 <= d <= last_day]
            if valid_days:
                return datetime(year, month, valid_days[0], self.hour, self.minute, self.second, tz=tz)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        next_run = self._get_next_run_date(run_after)
        return DataInterval(start=next_run, end=next_run.add(hours=1))

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction
    ) -> DagRunInfo | None:
        tz = pendulum_timezone(self.tz)
        current_time = pendulum.now(tz)
        baseline = restriction.earliest or current_time
        if not restriction.catchup:
            baseline = max(baseline, current_time)

        if last_automated_data_interval is None:
            after = baseline
        else:
            after = last_automated_data_interval.end
            if not restriction.catchup and after < current_time:
                after = current_time

        next_run = self._get_next_run_date(after)

        end = next_run.add(hours=1)

        if restriction.latest is not None and next_run > restriction.latest:
            return None

        return DagRunInfo.interval(next_run, end)

class QuarterlyFirstDay(Timetable):
    """
    Timetable for scheduling a DAG to run on the first day of each quarter at a specific hour, minute, and second.

    **Parameters:**
    - `hour` (`int`): Hour of the day (0-23). Default is 0.
    - `minute` (`int`): Minute of the hour (0-59). Default is 0.
    - `second` (`int`): Second of the minute (0-59). Default is 0.
    - `tz` (`str`): Timezone string (default "America/New_York").

    **Examples:**
    First day of each quarter at midnight:
        QuarterlyFirstDay()
    First day of each quarter at 08:30:
        QuarterlyFirstDay(hour=8, minute=30)
    """

    def __init__(self, hour: int = 0, minute: int = 0, tz: str = "America/New_York", second: int = 0):
        self.tz = tz
        self.hour = hour
        self.minute = minute
        self.second = second
        self.description = f" Quarterly, First of the Quarter at {self.hour:02d}:{self.minute:02d} ({self.tz})"

    def serialize(self):
        return {
            "tz": self.tz,
            "hour": self.hour,
            "minute": self.minute,
            "second": self.second,
        }

    @classmethod
    def deserialize(cls, data):
        return cls(
            tz=data.get("tz", "America/New_York"),
            hour=data.get("hour", 0),
            minute=data.get("minute", 0),
            second=data.get("second", 0),
        )

    def _get_first_day_of_quarter(self, year: int, month: int):
        tz = pendulum_timezone(self.tz)
        if month in [1, 2, 3]:
            quarter_month = 1
        elif month in [4, 5, 6]:
            quarter_month = 4
        elif month in [7, 8, 9]:
            quarter_month = 7
        else:
            quarter_month = 10
        return datetime(year, quarter_month, 1, self.hour, self.minute, self.second, tz=tz)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        year = run_after.year
        month = run_after.month
        first_day = self._get_first_day_of_quarter(year, month)
        if run_after < first_day:
            # Go to previous quarter
            if month in [1, 2, 3]:
                year -= 1
                month = 10
            elif month in [4, 5, 6]:
                month = 1
            elif month in [7, 8, 9]:
                month = 4
            else:
                month = 7
            first_day = self._get_first_day_of_quarter(year, month)
        start = first_day
        end = start.add(hours=1)
        return DataInterval(start=start, end=end)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction
    ) -> DagRunInfo | None:
        tz = pendulum_timezone(self.tz)
        current_time = pendulum.now(tz)
        baseline = restriction.earliest or current_time
        if not restriction.catchup:
            baseline = max(baseline, current_time)

        if last_automated_data_interval is None:
            next_run = baseline
        else:
            next_run = last_automated_data_interval.end
            if not restriction.catchup and next_run < current_time:
                next_run = current_time

        year = next_run.year
        month = next_run.month

        # Find the next first day of the quarter >= next_run
        while True:
            first_day = self._get_first_day_of_quarter(year, month)
            if first_day >= next_run:
                break
            # Move to next quarter
            if month in [1, 2, 3]:
                month = 4
            elif month in [4, 5, 6]:
                month = 7
            elif month in [7, 8, 9]:
                month = 10
            else:
                year += 1
                month = 1

        start = first_day
        end = start.add(hours=1)

        if restriction.latest is not None and start > restriction.latest:
            return None

        return DagRunInfo.interval(start, end)

class QuarterlyLastDay(Timetable):
    """
    Timetable for scheduling a DAG to run on the last day of each quarter at a specific hour/minute.

    **Parameters:**
    - `hour` (`int`): Hour of the day (0-23)
    - `minute` (`int`): Minute of the hour (0-59)
    - `second` (`int`): Second of the minute (default 0)
    - `tz` (`str`): Timezone string (default "America/New_York")
    """
    def __init__(self, hour: int = 0, minute: int = 0, tz: str = "America/New_York", second: int = 0):
        self.tz = tz
        self.hour = hour
        self.minute = minute
        self.second = second
        self.description = f" Quarterly, Last day of the Quarter at {self.hour:02d}:{self.minute:02d} ({self.tz})"

    def serialize(self):
        return {
            "tz": self.tz,
            "hour": self.hour,
            "minute": self.minute,
            "second": self.second,
        }

    @classmethod
    def deserialize(cls, data):
        return cls(
            tz=data.get("tz", "America/New_York"),
            hour=data.get("hour", 0),
            minute=data.get("minute", 0),
            second=data.get("second", 0),
        )

    def _get_last_day_of_quarter(self, year: int, month: int):
        tz = pendulum_timezone(self.tz)
        # Find the last month of the current quarter
        if month in [1, 2, 3]:
            last_month = 3
        elif month in [4, 5, 6]:
            last_month = 6
        elif month in [7, 8, 9]:
            last_month = 9
        else:
            last_month = 12
        # Get the first day of the next month, then subtract one day
        if last_month == 12:
            next_month = datetime(year + 1, 1, 1, tz=tz)
        else:
            next_month = datetime(year, last_month + 1, 1, tz=tz)
        last_day = next_month.subtract(days=1)
        return last_day.replace(hour=self.hour, minute=self.minute, second=self.second, microsecond=0)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        year = run_after.year
        month = run_after.month
        last_day = self._get_last_day_of_quarter(year, month)
        if run_after < last_day:
            # Go to previous quarter
            if month in [1, 2, 3]:
                year -= 1
                month = 12
            elif month in [4, 5, 6]:
                month = 3
            elif month in [7, 8, 9]:
                month = 6
            else:
                month = 9
            last_day = self._get_last_day_of_quarter(year, month)
        start = last_day
        end = start.add(hours=1)
        return DataInterval(start=start, end=end)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction
    ) -> DagRunInfo | None:
        tz = pendulum_timezone(self.tz)
        current_time = pendulum.now(tz)
        baseline = restriction.earliest or current_time
        if not restriction.catchup:
            baseline = max(baseline, current_time)

        if last_automated_data_interval is None:
            next_run = baseline
        else:
            next_run = last_automated_data_interval.end
            if not restriction.catchup and next_run < current_time:
                next_run = current_time

        year = next_run.year
        month = next_run.month

        # Find the next last day of the quarter >= next_run
        while True:
            last_day = self._get_last_day_of_quarter(year, month)
            if last_day >= next_run:
                break
            # Move to next quarter
            if month in [1, 2, 3]:
                month = 4
            elif month in [4, 5, 6]:
                month = 7
            elif month in [7, 8, 9]:
                month = 10
            else:
                year += 1
                month = 1

        start = last_day
        end = start.add(hours=1)

        if restriction.latest is not None and start > restriction.latest:
            return None

        return DagRunInfo.interval(start, end)

class YearlyFirstDay(Timetable):
    """
    Timetable for scheduling a DAG to run on the first day of each year at a specific hour, minute, and second.

    **Parameters:**
    - `hour` (`int`): Hour of the day (0-23). Default is 0.
    - `minute` (`int`): Minute of the hour (0-59). Default is 0.
    - `second` (`int`): Second of the minute (0-59). Default is 0.
    - `tz` (`str`): Timezone string (default "America/New_York").

    **Examples:**
    First day of each year at midnight:
        YearlyFirstDay()
    First day of each year at 08:30:
        YearlyFirstDay(hour=8, minute=30)
    """

    def __init__(self, hour: int = 0, minute: int = 0, tz: str = "America/New_York", second: int = 0):
        self.tz = tz
        self.hour = hour
        self.minute = minute
        self.second = second
        self.description = f" Yearly, First day of the Year at {self.hour:02d}:{self.minute:02d} ({self.tz})"


    def serialize(self):
        return {
            "tz": self.tz,
            "hour": self.hour,
            "minute": self.minute,
            "second": self.second,
        }

    @classmethod
    def deserialize(cls, data):
        return cls(
            tz=data.get("tz", "America/New_York"),
            hour=data.get("hour", 0),
            minute=data.get("minute", 0),
            second=data.get("second", 0),
        )

    def _get_first_day_of_year(self, year: int):
        tz = pendulum_timezone(self.tz)
        return datetime(year, 1, 1, self.hour, self.minute, self.second, tz=tz)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        year = run_after.year
        first_day = self._get_first_day_of_year(year)
        if run_after < first_day:
            year -= 1
            first_day = self._get_first_day_of_year(year)
        start = first_day
        end = start.add(hours=1)
        return DataInterval(start=start, end=end)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction
    ) -> DagRunInfo | None:
        tz = pendulum_timezone(self.tz)
        current_time = pendulum.now(tz)
        baseline = restriction.earliest or current_time
        if not restriction.catchup:
            baseline = max(baseline, current_time)

        if last_automated_data_interval is None:
            next_run = baseline
        else:
            next_run = last_automated_data_interval.end
            if not restriction.catchup and next_run < current_time:
                next_run = current_time

        year = next_run.year

        # Find the next first day of the year >= next_run
        while True:
            first_day = self._get_first_day_of_year(year)
            if first_day >= next_run:
                break
            year += 1

        start = first_day
        end = start.add(hours=1)

        if restriction.latest is not None and start > restriction.latest:
            return None

        return DagRunInfo.interval(start, end)

class YearlyWeekdayOccurrence(Timetable):
    """
    Timetable for scheduling a DAG to run on the Nth occurrence of a weekday in a specific month each year.

    **Parameters:**
    - `month` (`int`): Month of the year (1-12)
    - `weekday` (`int`): Day of the week (Monday=0, ..., Sunday=6)
    - `n` (`int`): Nth occurrence in the month (e.g., 1=first, 2=second, -1=last)
    - `hour` (`int`): Hour of the day (0-23). Default is 0.
    - `minute` (`int`): Minute of the hour (0-59). Default is 0.
    - `second` (`int`): Second of the minute (0-59). Default is 0.
    - `tz` (`str`): Timezone string (default "America/New_York").

    **Examples:**
    First Monday of November at 09:00:
        YearlyWeekdayOccurrence(month=11, weekday=0, n=1, hour=9, minute=0)
    Last Friday of December at 17:30:
        YearlyWeekdayOccurrence(month=12, weekday=4, n=-1, hour=17, minute=30)
    """

    def __init__(self, month: int, weekday: int, n: int, hour: int = 0, minute: int = 0, tz: str = "America/New_York", second: int = 0):
        self.month = month
        self.weekday = weekday
        self.n = n
        self.tz = tz
        self.hour = hour
        self.minute = minute
        self.second = second

    def _get_nth_weekday(self, year: int):
        tz = pendulum_timezone(self.tz)
        if self.n > 0:
            dt = datetime(year, self.month, 1, tz=tz)
            count = 0
            while True:
                if dt.day_of_week == self.weekday:
                    count += 1
                    if count == self.n:
                        break
                dt = dt.add(days=1)
            return dt.replace(hour=self.hour, minute=self.minute, second=self.second, microsecond=0)
        else:
            if self.month == 12:
                dt = datetime(year + 1, 1, 1, tz=tz).subtract(days=1)
            else:
                dt = datetime(year, self.month + 1, 1, tz=tz).subtract(days=1)
            count = -1
            while True:
                if dt.day_of_week == self.weekday:
                    if self.n == count:
                        break
                    count -= 1
                dt = dt.subtract(days=1)
            return dt.replace(hour=self.hour, minute=self.minute, second=self.second, microsecond=0)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        year = run_after.year
        nth_weekday = self._get_nth_weekday(year)
        if run_after < nth_weekday:
            year -= 1
            nth_weekday = self._get_nth_weekday(year)
        start = nth_weekday
        end = start.add(hours=1)
        return DataInterval(start=start, end=end)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction
    ) -> DagRunInfo | None:
        tz = pendulum_timezone(self.tz)
        current_time = pendulum.now(tz)
        baseline = restriction.earliest or current_time
        if not restriction.catchup:
            baseline = max(baseline, current_time)

        if last_automated_data_interval is None:
            next_run = baseline
        else:
            next_run = last_automated_data_interval.end
            if not restriction.catchup and next_run < current_time:
                next_run = current_time

        year = next_run.year

        # Find the next nth weekday in the configured month >= next_run
        while True:
            nth_weekday = self._get_nth_weekday(year)
            if nth_weekday >= next_run:
                break
            year += 1

        start = nth_weekday
        end = start.add(hours=1)

        if restriction.latest is not None and start > restriction.latest:
            return None
        
        return DagRunInfo.interval(start, end) 

class WeeklyOnDay(Timetable):
    """
    Timetable for scheduling a DAG to run weekly on a specific weekday at a specific hour/minute.

    **Parameters:**
    - `weekday` (`int`): Day of the week (Monday=0, ..., Sunday=6)
    - `hour` (`int`): Hour of the day (0-23)
    - `minute` (`int`): Minute of the hour (0-59)
    - `second` (`int`): Second of the minute (default 0)
    - `tz` (`str`): Timezone string (default "America/New_York")

    **Examples:**
    Every Monday at 08:00:
        WeeklyOnDay(weekday=0, hour=8, minute=0)
    Every Friday at 17:30:
        WeeklyOnDay(weekday=4, hour=17, minute=30)
    """

    WEEKDAY_NAMES = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]

    def __init__(self, weekday: int = 0, hour: int = 0, minute: int = 0, tz: str = "America/New_York", second: int = 0):
        self.weekday = weekday  # Monday=0, Sunday=6
        self.tz = tz
        self.hour = hour
        self.minute = minute
        self.second = second

    @property
    def description(self):
        # Always use weekday name, fallback to number if out of range
        try:
            weekday_str = self.WEEKDAY_NAMES[int(self.weekday)]
        except (IndexError, ValueError, TypeError):
            weekday_str = str(self.weekday)
        return f" Weekly, On {weekday_str} at {self.hour:02d}:{self.minute:02d} ({self.tz})"

    def serialize(self):
        return {
            "weekday": self.weekday,
            "tz": self.tz,
            "hour": self.hour,
            "minute": self.minute,
            "second": self.second,
        }

    @classmethod
    def deserialize(cls, data):
        return cls(
            weekday=data.get("weekday", 0),
            tz=data.get("tz", "America/New_York"),
            hour=data.get("hour", 0),
            minute=data.get("minute", 0),
            second=data.get("second", 0),
        )

    def _get_next_weekday(self, after):
        # after: pendulum DateTime
        days_ahead = (self.weekday - after.day_of_week + 7) % 7
        if days_ahead == 0 and after.hour >= self.hour and after.minute >= self.minute:
            days_ahead = 7
        return after.add(days=days_ahead).replace(hour=self.hour, minute=self.minute, second=self.second, microsecond=0)

    def infer_manual_data_interval(self, *, run_after):
        next_run = self._get_next_weekday(run_after)
        return DataInterval(start=next_run, end=next_run.add(hours=1))

    def next_dagrun_info(self, *, last_automated_data_interval, restriction):
        tz = pendulum_timezone(self.tz)
        current_time = datetime.now(tz)
        baseline = restriction.earliest or current_time
        if not restriction.catchup:
            baseline = max(baseline, current_time)
        if last_automated_data_interval is None:
            after = baseline
        else:
            after = last_automated_data_interval.end
            if not restriction.catchup and after < current_time:
                after = current_time
        next_run = self._get_next_weekday(after)
        return DagRunInfo.interval(next_run, next_run.add(hours=1))

class BiweeklyOnDay(Timetable):
    """
    Timetable for scheduling a DAG to run every two weeks on a specific weekday at a specific hour/minute.

    **Parameters:**
    - `weekday` (`int`): Day of the week (Monday=0, ..., Sunday=6)
    - `hour` (`int`): Hour of the day (0-23)
    - `minute` (`int`): Minute of the hour (0-59)
    - `second` (`int`): Second of the minute (default 0)
    - `tz` (`str`): Timezone string (default "America/New_York")
    - `anchor_date` (`str`): ISO date string (YYYY-MM-DD) to anchor the biweekly cycle (default: "2024-01-01")

    **Examples:**
    Every other Monday at 08:00:
        BiweeklyOnDay(weekday=0, hour=8, minute=0)
    """

    def __init__(
        self,
        weekday: int = 0,
        hour: int = 0,
        minute: int = 0,
        tz: str = "America/New_York",
        second: int = 0,
        anchor_date: str = "2024-01-01"
    ):
        self.weekday = weekday
        self.tz = tz
        self.hour = hour
        self.minute = minute
        self.second = second
        self.anchor_date = anchor_date
        self.description = (
            f"Biweekly, every other {['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'][self.weekday]} "
            f"at {self.hour:02d}:{self.minute:02d} ({self.tz}), anchor: {self.anchor_date}"
        )

    def serialize(self):
        return {
            "weekday": self.weekday,
            "tz": self.tz,
            "hour": self.hour,
            "minute": self.minute,
            "second": self.second,
            "anchor_date": self.anchor_date,
        }

    @classmethod
    def deserialize(cls, data):
        return cls(
            weekday=data.get("weekday", 0),
            tz=data.get("tz", "America/New_York"),
            hour=data.get("hour", 0),
            minute=data.get("minute", 0),
            second=data.get("second", 0),
            anchor_date=data.get("anchor_date", "2024-01-01"),
        )

    def _get_next_biweekly(self, after):
        tz = pendulum_timezone(self.tz)
        anchor = pendulum.parse(self.anchor_date, tz=tz)
        # Find the first anchor weekday on or after anchor_date
        days_to_weekday = (self.weekday - anchor.day_of_week + 7) % 7
        first_run = anchor.add(days=days_to_weekday).replace(
            hour=self.hour, minute=self.minute, second=self.second, microsecond=0
        )
        # Calculate number of weeks since anchor
        if after < first_run:
            return first_run
        delta_days = (after - first_run).days
        cycles = (delta_days // 14) + (0 if after <= first_run.add(days=13, hours=23, minutes=59, seconds=59) else 1)
        next_run = first_run.add(weeks=cycles * 2)
        # If next_run is before or equal to 'after', move to next cycle
        if next_run <= after:
            next_run = next_run.add(weeks=2)
        return next_run

    def infer_manual_data_interval(self, *, run_after):
        next_run = self._get_next_biweekly(run_after)
        return DataInterval(start=next_run, end=next_run.add(hours=1))

    def next_dagrun_info(self, *, last_automated_data_interval, restriction):
        tz = pendulum_timezone(self.tz)
        current_time = datetime.now(tz)
        baseline = restriction.earliest or current_time
        if not restriction.catchup:
            baseline = max(baseline, current_time)
        if last_automated_data_interval is None:
            after = baseline
        else:
            after = last_automated_data_interval.end
            if not restriction.catchup and after < current_time:
                after = current_time
        next_run = self._get_next_biweekly(after)
        return DagRunInfo.interval(next_run, next_run.add(hours=1))

class SemiMonthly(Timetable):
    """
    Timetable for scheduling a DAG to run semi-monthly: on the 15th and the last day of each month at a specific hour/minute.

    **Parameters:**
    - `hour` (`int`): Hour of the day (0-23)
    - `minute` (`int`): Minute of the hour (0-59)
    - `second` (`int`): Second of the minute (default 0)
    - `tz` (`str`): Timezone string (default "America/New_York")

    **Examples:**
    15th and last day of every month at 08:00:
        SemiMonthly(hour=8, minute=0)
    """

    def __init__(self, hour: int = 0, minute: int = 0, tz: str = "America/New_York", second: int = 0):
        self.tz = tz
        self.hour = hour
        self.minute = minute
        self.second = second
        self.description = (
            f"Semi-monthly, 15th and last day at {self.hour:02d}:{self.minute:02d} ({self.tz})"
        )

    def serialize(self):
        return {
            "tz": self.tz,
            "hour": self.hour,
            "minute": self.minute,
            "second": self.second,
        }

    @classmethod
    def deserialize(cls, data):
        return cls(
            tz=data.get("tz", "America/New_York"),
            hour=data.get("hour", 0),
            minute=data.get("minute", 0),
            second=data.get("second", 0),
        )

    def _get_next_run_date(self, after):
        tz = pendulum_timezone(self.tz)
        year = after.year
        month = after.month

        # Find the 15th and last day of this month
        fifteenth = datetime(year, month, 15, self.hour, self.minute, self.second, tz=tz)
        if month == 12:
            last_day = (datetime(year + 1, 1, 1, tz=tz) - pendulum.duration(days=1)).day
        else:
            last_day = (datetime(year, month + 1, 1, tz=tz) - pendulum.duration(days=1)).day
        last = datetime(year, month, last_day, self.hour, self.minute, self.second, tz=tz)

        # Find the next run date in this month
        candidates = [fifteenth, last]
        candidates = [dt for dt in candidates if dt >= after]
        if candidates:
            return min(candidates)
        # If not found, go to next month and repeat
        while True:
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1
            fifteenth = datetime(year, month, 15, self.hour, self.minute, self.second, tz=tz)
            if month == 12:
                last_day = (datetime(year + 1, 1, 1, tz=tz) - pendulum.duration(days=1)).day
            else:
                last_day = (datetime(year, month + 1, 1, tz=tz) - pendulum.duration(days=1)).day
            last = datetime(year, month, last_day, self.hour, self.minute, self.second, tz=tz)
            return min(fifteenth, last)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        next_run = self._get_next_run_date(run_after)
        return DataInterval(start=next_run, end=next_run.add(hours=1))

    def next_dagrun_info(self, *, last_automated_data_interval, restriction):
        tz = pendulum_timezone(self.tz)
        current_time = datetime.now(tz)
        baseline = restriction.earliest or current_time
        if not restriction.catchup:
            baseline = max(baseline, current_time)
        if last_automated_data_interval is None:
            after = baseline
        else:
            after = last_automated_data_interval.end
            if not restriction.catchup and after < current_time:
                after = current_time
        next_run = self._get_next_run_date(after)
        return DagRunInfo.interval(next_run, next_run.add(hours=1))

class MonthlyWeekdayOccurrence(Timetable):
    """
    Timetable for scheduling a DAG to run on the Nth occurrence of a weekday in each month at a specific hour/minute.

    **Parameters:**
    - `weekday` (`int`): Day of the week (Monday=0, ..., Sunday=6)
    - `n` (`int`): Nth occurrence in the month (e.g., 1=first, 2=second, -1=last)
    - `hour` (`int`): Hour of the day (0-23)
    - `minute` (`int`): Minute of the hour (0-59)
    - `second` (`int`): Second of the minute (default 0)
    - `tz` (`str`): Timezone string (default "America/New_York")

    **Examples:**
    Second Tuesday of every month at 08:00:
        MonthlyWeekdayOccurrence(weekday=1, n=2, hour=8, minute=0)
    Last Friday of every month at 17:30:
        MonthlyWeekdayOccurrence(weekday=4, n=-1, hour=17, minute=30)
    """
    def __init__(self, weekday: int = 0, n: int = 1, hour: int = 0, minute: int = 0, tz: str = "America/New_York", second: int = 0):
        self.weekday = weekday
        self.n = n
        self.tz = tz
        self.hour = hour
        self.minute = minute
        self.second = second
        nth_str = {1: "First", 2: "Second", 3: "Third", 4: "Fourth", -1: "Last"}.get(n, f"{n}th")
        self.description = f" Monthly, {nth_str} {['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'][self.weekday]} at {self.hour:02d}:{self.minute:02d} ({self.tz})"

    def serialize(self):
        return {
            "weekday": self.weekday,
            "n": self.n,
            "tz": self.tz,
            "hour": self.hour,
            "minute": self.minute,
            "second": self.second,
        }

    @classmethod
    def deserialize(cls, data):
        return cls(
            weekday=data.get("weekday", 0),
            n=data.get("n", 1),
            tz=data.get("tz", "America/New_York"),
            hour=data.get("hour", 0),
            minute=data.get("minute", 0),
            second=data.get("second", 0),
        )

    def _get_nth_weekday(self, year: int, month: int):
        tz = pendulum_timezone(self.tz)
        if self.n > 0:
            # Find the first day of the month
            dt = datetime(year, month, 1, tz=tz)
            count = 0
            while True:
                if dt.day_of_week == self.weekday:
                    count += 1
                    if count == self.n:
                        break
                dt = dt.add(days=1)
            return dt.replace(hour=self.hour, minute=self.minute, second=self.second, microsecond=0)
        else:
            # Find the last day of the month
            if month == 12:
                dt = datetime(year + 1, 1, 1, tz=tz).subtract(days=1)
            else:
                dt = datetime(year, month + 1, 1, tz=tz).subtract(days=1)
            count = -1
            while True:
                if dt.day_of_week == self.weekday:
                    if self.n == count:
                        break
                    count -= 1
                dt = dt.subtract(days=1)
            return dt.replace(hour=self.hour, minute=self.minute, second=self.second, microsecond=0)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        year = run_after.year
        month = run_after.month
        nth_weekday = self._get_nth_weekday(year, month)
        if run_after < nth_weekday:
            # Go to previous month
            if month == 1:
                year -= 1
                month = 12
            else:
                month -= 1
            nth_weekday = self._get_nth_weekday(year, month)
        start = nth_weekday
        end = start.add(hours=1)
        return DataInterval(start=start, end=end)

    def next_dagrun_info(self, *, last_automated_data_interval: DataInterval | None, restriction: TimeRestriction) -> DagRunInfo | None:
        tz = pendulum_timezone(self.tz)
        current_time = datetime.now(tz)
        baseline = restriction.earliest or current_time
        if not restriction.catchup:
            baseline = max(baseline, current_time)
        if last_automated_data_interval is None:
            next_run = baseline
        else:
            next_run = last_automated_data_interval.end
            if not restriction.catchup and next_run < current_time:
                next_run = current_time

        year = next_run.year
        month = next_run.month

        while True:
            nth_weekday = self._get_nth_weekday(year, month)
            if nth_weekday >= next_run:
                break
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1

        start = nth_weekday
        end = start.add(hours=1)
        return DagRunInfo.interval(start, end)

class EveryNDays(Timetable):
    """
    Timetable for scheduling a DAG to run every Number (N) of days at a specific hour/minute, regardless of month or week boundaries.

    **Parameters:**
    - `interval_days` (`int`): Number of days between runs (e.g., 10 for every 10 days)
    - `hour` (`int`): Hour of the day (0-23)
    - `minute` (`int`): Minute of the hour (0-59)
    - `second` (`int`): Second of the minute (default 0)
    - `tz` (`str`): Timezone string (default "America/New_York")

    **Examples:**
    Every 10 days at 07:00:
        EveryNDays(interval_days=10, hour=7, minute=0)
    Every 3 days at 23:30:
        EveryNDays(interval_days=3, hour=23, minute=30)
    """
    def __init__(self, interval_days: int = 1, hour: int = 0, minute: int = 0, tz: str = "America/New_York", second: int = 0):
        self.interval_days = interval_days
        self.tz = tz
        self.hour = hour
        self.minute = minute
        self.second = second
        self.description = f" Every {self.interval_days} days at {self.hour:02d}:{self.minute:02d} ({self.tz})"

    def serialize(self):
        return {
            "interval_days": self.interval_days,
            "tz": self.tz,
            "hour": self.hour,
            "minute": self.minute,
            "second": self.second,
        }

    @classmethod
    def deserialize(cls, data):
        return cls(
            interval_days=data.get("interval_days", 1),
            tz=data.get("tz", "America/New_York"),
            hour=data.get("hour", 0),
            minute=data.get("minute", 0),
            second=data.get("second", 0),
        )

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        tz = pendulum_timezone(self.tz)
        # Find the most recent run time before or equal to run_after
        base = run_after.in_timezone(tz).replace(hour=self.hour, minute=self.minute, second=self.second, microsecond=0)
        while base > run_after:
            base = base.subtract(days=self.interval_days)
        start = base
        end = start.add(days=self.interval_days)
        return DataInterval(start=start, end=end)

    def next_dagrun_info(self, *, last_automated_data_interval: DataInterval | None, restriction: TimeRestriction) -> DagRunInfo | None:
        tz = pendulum_timezone(self.tz)
        current_time = datetime.now(tz)
        baseline = restriction.earliest or current_time
        if not restriction.catchup:
            baseline = max(baseline, current_time)
        if last_automated_data_interval is None:
            next_run = baseline
        else:
            next_run = last_automated_data_interval.end
            if not restriction.catchup and next_run < current_time:
                next_run = current_time
        # Align to the next scheduled time on or after next_run
        aligned = next_run.replace(hour=self.hour, minute=self.minute, second=self.second, microsecond=0)
        if aligned < next_run:
            aligned = aligned.add(days=1)
        start = aligned
        end = start.add(days=self.interval_days)
        return DagRunInfo.interval(start, end)

class BusinessDayOfMonth(Timetable):
    """
    Timetable for scheduling a DAG to run on the Nth or last business (weekday) day of each month at a specific hour/minute.

    **Parameters:**
    - `n` (`int`): Nth business day (1=first, 2=second, ..., -1=last)
    - `hour` (`int`): Hour of the day (0-23)
    - `minute` (`int`): Minute of the hour (0-59)
    - `second` (`int`): Second of the minute (default 0)
    - `tz` (`str`): Timezone string (default "America/New_York")

    **Examples:**
    First business day of each month at 09:00:
        BusinessDayOfMonth(n=1, hour=9, minute=0)
    Last business day of each month at 17:00:
        BusinessDayOfMonth(n=-1, hour=17, minute=0)
    """
    def __init__(self, n: int = 1, hour: int = 0, minute: int = 0, tz: str = "America/New_York", second: int = 0):
        self.n = n
        self.tz = tz
        self.hour = hour
        self.minute = minute
        self.second = second
        nth_str = {1: "First", 2: "Second", 3: "Third", 4: "Fourth", -1: "Last"}.get(n, f"{n}th")
        self.description = f" Monthly, {nth_str} business day at {self.hour:02d}:{self.minute:02d} ({self.tz})"

    def serialize(self):
        return {
            "n": self.n,
            "tz": self.tz,
            "hour": self.hour,
            "minute": self.minute,
            "second": self.second,
        }

    @classmethod
    def deserialize(cls, data):
        return cls(
            n=data.get("n", 1),
            tz=data.get("tz", "America/New_York"),
            hour=data.get("hour", 0),
            minute=data.get("minute", 0),
            second=data.get("second", 0),
        )

    def _get_nth_business_day(self, year: int, month: int):
        tz = pendulum_timezone(self.tz)
        if self.n > 0:
            dt = datetime(year, month, 1, tz=tz)
            count = 0
            while True:
                if dt.day_of_week < 5:  # Monday=0, ..., Friday=4
                    count += 1
                    if count == self.n:
                        break
                dt = dt.add(days=1)
        else:
            # Last business day
            if month == 12:
                dt = datetime(year + 1, 1, 1, tz=tz).subtract(days=1)
            else:
                dt = datetime(year, month + 1, 1, tz=tz).subtract(days=1)
            count = -1
            while True:
                if dt.day_of_week < 5:
                    if self.n == count:
                        break
                    count -= 1
                dt = dt.subtract(days=1)
        return dt.replace(hour=self.hour, minute=self.minute, second=self.second, microsecond=0)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        year = run_after.year
        month = run_after.month
        nth_business_day = self._get_nth_business_day(year, month)
        if run_after < nth_business_day:
            # Go to previous month
            if month == 1:
                year -= 1
                month = 12
            else:
                month -= 1
            nth_business_day = self._get_nth_business_day(year, month)
        start = nth_business_day
        end = start.add(hours=1)
        return DataInterval(start=start, end=end)

    def next_dagrun_info(self, *, last_automated_data_interval: DataInterval | None, restriction: TimeRestriction) -> DagRunInfo | None:
        tz = pendulum_timezone(self.tz)
        current_time = datetime.now(tz)
        baseline = restriction.earliest or current_time
        if not restriction.catchup:
            baseline = max(baseline, current_time)
        if last_automated_data_interval is None:
            next_run = baseline
        else:
            next_run = last_automated_data_interval.end
            if not restriction.catchup and next_run < current_time:
                next_run = current_time

        year = next_run.year
        month = next_run.month

        while True:
            nth_business_day = self._get_nth_business_day(year, month)
            if nth_business_day >= next_run:
                break
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1

        start = nth_business_day
        end = start.add(hours=1)
        return DagRunInfo.interval(start, end)

class MonthlyLastDayExceptWeekend(Timetable):
    """
    Timetable for scheduling a DAG to run on the last day of each month at a specific hour/minute,
    but if the last day is a Saturday or Sunday, move to the previous Friday.

    **Parameters:**
    - `hour` (`int`): Hour of the day (0-23)
    - `minute` (`int`): Minute of the hour (0-59)
    - `second` (`int`): Second of the minute (default 0)
    - `tz` (`str`): Timezone string (default "America/New_York")

    **Examples:**
    Last day of each month at 18:00, or previous Friday if weekend:
        LastDayExceptWeekend(hour=18, minute=0)
    """
    def __init__(self, hour: int = 0, minute: int = 0, tz: str = "America/New_York", second: int = 0):
        self.tz = tz
        self.hour = hour
        self.minute = minute
        self.second = second
        self.description = f" Monthly, Last day (or previous Friday if weekend) at {self.hour:02d}:{self.minute:02d} ({self.tz})"

    def serialize(self):
        return {
            "tz": self.tz,
            "hour": self.hour,
            "minute": self.minute,
            "second": self.second,
        }

    @classmethod
    def deserialize(cls, data):
        return cls(
            tz=data.get("tz", "America/New_York"),
            hour=data.get("hour", 0),
            minute=data.get("minute", 0),
            second=data.get("second", 0),
        )

    def _get_last_day(self, year: int, month: int):
        tz = pendulum_timezone(self.tz)
        if month == 12:
            next_month = datetime(year + 1, 1, 1, tz=tz)
        else:
            next_month = datetime(year, month + 1, 1, tz=tz)
        last_day = next_month.subtract(days=1)
        # If last day is Saturday (5) or Sunday (6), move to previous Friday (4)
        while last_day.day_of_week > 4:
            last_day = last_day.subtract(days=1)
        return last_day.replace(hour=self.hour, minute=self.minute, second=self.second, microsecond=0)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        year = run_after.year
        month = run_after.month
        last_day = self._get_last_day(year, month)
        if run_after < last_day:
            # Go to previous month
            if month == 1:
                year -= 1
                month = 12
            else:
                month -= 1
            last_day = self._get_last_day(year, month)
        start = last_day
        end = start.add(hours=1)
        return DataInterval(start=start, end=end)

    def next_dagrun_info(self, *, last_automated_data_interval: DataInterval | None, restriction: TimeRestriction) -> DagRunInfo | None:
        tz = pendulum_timezone(self.tz)
        current_time = datetime.now(tz)
        baseline = restriction.earliest or current_time
        if not restriction.catchup:
            baseline = max(baseline, current_time)
        if last_automated_data_interval is None:
            next_run = baseline
        else:
            next_run = last_automated_data_interval.end
            if not restriction.catchup and next_run < current_time:
                next_run = current_time

        year = next_run.year
        month = next_run.month

        while True:
            last_day = self._get_last_day(year, month)
            if last_day >= next_run:
                break
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1

        start = last_day
        end = start.add(hours=1)
        return DagRunInfo.interval(start, end)

class CronTimetable(CronDataIntervalTimetable):
    """
    Timetable for scheduling a DAG using any cron expression.

    **Parameters:**
    - `cron` (`str`): Cron expression (e.g., "0 9 15 * *" for 15th of every month at 09:00)
    - `timezone` (`str`): Timezone string (default "America/New_York")

    **Examples:**
    15th of every month at 09:00:
        CronTimetable("0 9 15 * *", timezone="America/New_York")
    Every Monday and Wednesday at 18:30:
        CronTimetable("30 18 * * 1,3", timezone="America/New_York")
    """

    def __init__(self, cron: str, timezone: str = "America/New_York"):
        super().__init__(cron, timezone=timezone)
        self.cron = cron
        self.timezone = timezone
        self.description = f"Cron: {self.cron} ({self.timezone})"

    def serialize(self):
        return {
            "cron": self.cron,
            "timezone": self.timezone,
        }

    @classmethod
    def deserialize(cls, data):
        return cls(
            cron=data.get("cron"),
            timezone=data.get("timezone", "America/New_York"),
        )

class EveryNInterval(Timetable):
    """
    Timetable for scheduling a DAG to run every N minutes and/or N hours, regardless of day boundaries.

    **Parameters:**
    - `interval_minutes` (`int`): Number of minutes between runs (e.g., 15 for every 15 minutes)
    - `interval_hours` (`int`): Number of hours between runs (e.g., 6 for every 6 hours)
    - `tz` (`str`): Timezone string (default "America/New_York")

    **Examples:**
    Every 2 minutes:
        EveryNInterval(interval_minutes=2, tz="America/New_York")
    Every 6 hours:
        EveryNInterval(interval_hours=6, tz="America/New_York")
    Every 1 hour and 30 minutes:
        EveryNInterval(interval_hours=1, interval_minutes=30, tz="America/New_York")
    """
    def __init__(self, interval_minutes=0, interval_hours=0, tz="America/New_York"):
        self.interval_minutes = interval_minutes
        self.interval_hours = interval_hours
        self.tz = tz

    @property
    def description(self):
        parts = []
        if self.interval_hours:
            parts.append(f"{self.interval_hours} hour{'s' if self.interval_hours != 1 else ''}")
        if self.interval_minutes:
            parts.append(f"{self.interval_minutes} minute{'s' if self.interval_minutes != 1 else ''}")
        return f"Every {' and '.join(parts)} ({self.tz})"

    def serialize(self):
        return {
            "interval_minutes": self.interval_minutes,
            "interval_hours": self.interval_hours,
            "tz": self.tz,
        }

    @classmethod
    def deserialize(cls, data):
        # Provide defaults if missing
        return cls(
            interval_minutes=data.get("interval_minutes", 0),
            interval_hours=data.get("interval_hours", 0),
            tz=data.get("tz", "America/New_York"),
        )

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        total_minutes = self.interval_hours * 60 + self.interval_minutes
        if total_minutes == 0:
            raise ValueError("At least one of interval_minutes or interval_hours must be > 0")
        interval = duration(minutes=total_minutes)
        anchor = run_after.replace(hour=0, minute=0, second=0, microsecond=0)
        minutes_since_anchor = int((run_after - anchor).total_minutes())
        aligned_minutes = (minutes_since_anchor // total_minutes) * total_minutes
        aligned = anchor.add(minutes=aligned_minutes)
        return DataInterval(start=aligned, end=aligned + interval)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        total_minutes = self.interval_hours * 60 + self.interval_minutes
        if total_minutes == 0:
            raise ValueError("At least one of interval_minutes or interval_hours must be > 0")
        interval = duration(minutes=total_minutes)
        tz = self.tz
        current_time = now(tz)
        baseline = restriction.earliest or current_time
        if not restriction.catchup:
            baseline = max(baseline, current_time)
        anchor = baseline.replace(hour=0, minute=0, second=0, microsecond=0)
        minutes_since_anchor = int((baseline - anchor).total_minutes())
        aligned_minutes = (minutes_since_anchor // total_minutes) * total_minutes
        aligned = anchor.add(minutes=aligned_minutes)
        if aligned < baseline:
            aligned = aligned.add(minutes=total_minutes)
        if last_automated_data_interval:
            next_start = last_automated_data_interval.end
            if next_start < aligned:
                next_start = aligned
        else:
            next_start = aligned
        if restriction.latest and next_start > restriction.latest:
            return None
        return DagRunInfo.interval(start=next_start, end=next_start + interval)

class CustomIntervalTimetables(AirflowPlugin):
    name = "CustomIntervalTimetables"
    timetables = [
        WeeklyOnDay,
        BiweeklyOnDay,
        MonthlyOnDay,
        MonthlyLastDay,
        MonthlyWeekdayOccurrence,
        MonthlyLastDayExceptWeekend,
        MonthlyMultipleDays,
        SemiMonthly,
        QuarterlyFirstDay,
        QuarterlyLastDay, 
        YearlyFirstDay,
        YearlyWeekdayOccurrence,
        EveryNDays,
        BusinessDayOfMonth,
        CronTimetable,
        EveryNInterval,
    ]
