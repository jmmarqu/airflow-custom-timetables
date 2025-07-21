# Custom Timetables

This package provides a set of custom Airflow timetables for advanced scheduling scenarios.
Below you'll find usage instructions and examples for each timetable class.

---

## How to Use

1. **Import the desired timetable class in your DAG file:**

   ```python
   from custom_timetables import (
       MonthlyLastDay, MonthlyOnDay, MonthlyMultipleDays, QuarterlyFirstDay, QuarterlyLastDay,
       YearlyFirstDay, YearlyWeekdayOccurrence, WeeklyOnDay, BiweeklyOnDay, SemiMonthly
   )
   ```
2. **Set the timetable in your DAG:**

   ```python
   from airflow import DAG

   dag = DAG(
       "my_dag",
       timetable=SemiMonthly(hour=8, minute=0),
   )
   ```

---

## Timetable Classes and Examples

| **Pattern**                        | **Class**                 | **Sample Usage**                                                        | **Description**                              |
| ---------------------------------------- | ------------------------------- | ----------------------------------------------------------------------------- | -------------------------------------------------- |
| Last day of month                        | `MonthlyLastDay`              | `MonthlyLastDay(hour=18, minute=0)`                                         | Last day of each month at 18:00                    |
| Specific day of month                    | `MonthlyOnDay`                | `MonthlyOnDay(day=10, hour=9, minute=0)`                                    | 10th of each month at 09:00                        |
| Multiple days in month                   | `MonthlyMultipleDays`         | `MonthlyMultipleDays(days=[1, 15], hour=8, minute=0)`                       | 1st and 15th of each month at 08:00                |
| First day of quarter                     | `QuarterlyFirstDay`           | `QuarterlyFirstDay(hour=7, minute=0)`                                       | First day of each quarter at 07:00                 |
| Last day of quarter                      | `QuarterlyLastDay`            | `QuarterlyLastDay(hour=17, minute=30)`                                      | Last day of each quarter at 17:30                  |
| First day of year                        | `YearlyFirstDay`              | `YearlyFirstDay(hour=0, minute=0)`                                          | January 1st each year at midnight                  |
| Nth weekday in month                     | `MonthlyWeekdayOccurrence`    | `MonthlyWeekdayOccurrence(weekday=0, n=2, hour=9, minute=0)`                | 2nd Monday of each month at 09:00                  |
| Nth weekday in year/month                | `YearlyWeekdayOccurrence`     | `YearlyWeekdayOccurrence(month=11, weekday=0, n=1, hour=9, minute=0)`       | 1st Monday of November at 09:00                    |
| Weekly on specific weekday               | `WeeklyOnDay`                 | `WeeklyOnDay(weekday=2, hour=10, minute=0)`                                 | Every Wednesday at 10:00                           |
| Biweekly on specific weekday             | `BiweeklyOnDay`               | `BiweeklyOnDay(weekday=4, hour=8, minute=0)`                                | Every other Friday at 08:00                        |
| Semi-monthly (15th and last day)         | `SemiMonthly`                 | `SemiMonthly(hour=8, minute=0)`                                             | 15th and last day of each month at 08:00           |
| Every N days                             | `EveryNDays`                  | `EveryNDays(interval_days=10, hour=7, minute=0)`                            | Every 10 days at 07:00                             |
| Nth business day of month                | `BusinessDayOfMonth`          | `BusinessDayOfMonth(n=1, hour=9, minute=0)`                                 | 1st business day of each month at 09:00            |
| Last business day of month               | `BusinessDayOfMonth`          | `BusinessDayOfMonth(n=-1, hour=17, minute=0)`                               | Last business day of each month at 17:00           |
| Last day except weekend (move to Friday) | `MonthlyLastDayExceptWeekend` | `MonthlyLastDayExceptWeekend(hour=18, minute=0)`                            | Last day of month at 18:00, or previous Friday     |
| Cron expression                          | `CronTimetable`               | `CronTimetable("0 9 15 * *", timezone="America/New_York")`                  | 15th of every month at 09:00 (or any cron pattern) |
| Every N hours/minutes                    | `EveryNInterval`              | `EveryNInterval(interval_hours=6)<br>``EveryNInterval(interval_minutes=45)` | Every 6 hours or every 45 minutes                  |

---

## Notes

- All timetables accept a `tz` or `timezone` parameter (default: `"America/New_York"`).
- For weekday parameters: Monday=0, ..., Sunday=6.
- For `MonthlyWeekdayOccurrence` and `YearlyWeekdayOccurrence`, `n=-1` means "last" occurrence.
- For `BusinessDayOfMonth`, `n=-1` means "last business day".

---

## CronTimetable Usage Examples

**For more advanced patterns, combine these classes or use `CronTimetable` for full cron flexibility.**

The `CronTimetable` class allows you to use any cron expression for scheduling.
You can specify the cron string and an optional timezone.

```python
from custom_timetables import CronTimetable

# 15th of every month at 09:00
timetable = CronTimetable("0 9 15 * *", timezone="America/New_York")

# Every Monday and Wednesday at 18:30
timetable = CronTimetable("30 18 * * 1,3", timezone="America/New_York")

# Every day at midnight UTC
timetable = CronTimetable("0 0 * * *", timezone="UTC")

# Every weekday (Mon-Fri) at 07:15
timetable = CronTimetable("15 7 * * 1-5", timezone="America/New_York")

# Every 10 minutes
timetable = CronTimetable("*/10 * * * *", timezone="America/New_York")
```

**Tip:**
You can use any valid cron expression supported by Airflow.
See [crontab.guru](https://crontab.guru/) for help building cron
