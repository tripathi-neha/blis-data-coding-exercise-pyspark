# Implementation Walkthrough

This document explains how `blis/main.py` implements the five requirements in the project README. The transformations use PySpark DataFrame expressions and Spark SQL window functions; there are no Python UDFs.

## 1. Filter invalid events

Function: `filter_invalid_records(input_df)`

The function applies both validation rules from requirement 1:

1. It casts `timestamp` to a numeric long value in a temporary column. This allows timestamps read as CSV strings to be compared numerically.
2. A window partitions rows by `user_id` and the numeric timestamp. `count` computes how many events share that pair.
3. The count is computed before checking `web_activity`. That way, an invalid event at a duplicated user/timestamp still causes every event at that timestamp to be discarded.
4. The function keeps rows with a count of one and a non-null, non-empty activity string that matches the allowed-character regex: letters, digits, pipe, comma, percent, dot, equals, underscore, and hyphen.
5. Temporary columns are removed before returning the DataFrame.

The implementation assumes event timestamps are valid epoch-microsecond values. The regex does not allow spaces.

## 2. Add activity codes

Function: `add_activity_lookup(input_df, lookup_df)`

1. It calls `filter_invalid_records`, so the lookup step works only with valid, unique events.
2. `split` separates `web_activity` on literal pipe characters. `explode` turns the tokens into rows temporarily.
3. A left join compares each token to the lookup `activity` column. This is an exact equality check, so `search` does not match `search1`.
4. The rows are grouped back by all original event columns. `max` reduces the matching lookup code to one value per event; this relies on the README guarantee that an event has at most one lookup match.
5. `coalesce` supplies `999` when there was no match.

The returned columns are the event columns plus `activity_code`.

## 3. Find each day's three busiest hours

Function: `get_top_3_active_hours(input_df)`

1. It filters invalid events.
2. `_epoch_micros_to_timestamp` converts the epoch-microsecond value to a Spark timestamp. The conversion uses Spark's session timezone; the test fixture sets it to UTC.
3. The function derives a calendar `day` and an integer `hour`, then counts events for each day/hour pair.
4. A window ranks hours within each day by descending count. If counts tie, the earlier hour comes first.
5. Rows ranked 1 through 3 are returned with `hour`, `day`, and `activity_count`.

Each day is ranked independently; a day with fewer than three observed hours returns fewer than three rows.

## 4. Find each day's two most-used activity codes

Function: `get_top_2_activity_codes(input_df, lookup_df)`

1. It calls `add_activity_lookup`, so invalid events are excluded and unmatched events receive code `999`.
2. It derives each event's calendar day and counts rows by day and `activity_code` across all users.
3. A window ranks codes within each day by descending count. If counts tie, the lower code comes first.
4. The top two rows per day are returned with `activity_code`, `day`, and `activity_count`.

## 5. Build user activity sessions

Function: `get_activity_sessions_per_user(input_df, lookup_df)`

1. It calls `add_activity_lookup` to use the same valid event set as the other downstream functions.
2. It casts the epoch-microsecond timestamp to a long and orders events within each user by that value.
3. `lag` retrieves the previous event timestamp. The first event starts a session; a gap of at least `3 * 60 * 60 * 1,000,000` microseconds starts another one.
4. A cumulative window sum assigns a session number to each event.
5. The function groups by user and session number and takes the minimum and maximum event timestamps. Session duration is elapsed time from the first event to the last event, not a sum of event durations.
6. The date of the minimum timestamp becomes `session_start_date`. Duration is divided by 60,000,000 and rounded down to whole minutes. A session with one event therefore has a duration of zero minutes.

The session date conversion follows Spark's session timezone, just like the daily hour and activity-code reports.

## Test expectations

The current tests in `test/test_app_functions.py` check these fixture results:

- `filter_invalid_records`: 200 rows.
- `add_activity_lookup`: 200 rows, including 24 rows with code `999`.
- `get_top_3_active_hours`: 3 rows, with hour values 6, 8, and 9 for the supplied fixture.
- `get_top_2_activity_codes`: 2 rows, with codes 1 and 3 for the supplied fixture.
- `get_activity_sessions_per_user`: 67 rows.

These checks use the provided data as-is. The tests do not currently cover every edge case, such as timestamp ties at exactly the three-hour boundary or tied daily counts.
