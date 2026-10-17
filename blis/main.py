"""
Main module
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window


APP_NAME = 'coding_example_run'


def _epoch_micros_to_timestamp(column):
  # Calendar boundaries follow Spark's session timezone (UTC in the tests).
  seconds = F.floor(column.cast('double') / F.lit(1_000_000)).cast('long')
  return F.to_timestamp(F.from_unixtime(seconds))


def filter_invalid_records(input_df: DataFrame) -> DataFrame:
  allowed_activity = r'^[A-Za-z0-9|,%.=_-]+$'
  timestamp_counts = Window.partitionBy('user_id', '_timestamp_micros')

  # Count duplicates first so invalid rows still invalidate their timestamp peers.
  return (
    input_df
    .withColumn('_timestamp_micros', F.col('timestamp').cast('long'))
    .withColumn('_timestamp_count', F.count(F.lit(1)).over(timestamp_counts))
    .filter(
      (F.col('_timestamp_count') == 1)
      & F.col('web_activity').isNotNull()
      & (F.col('web_activity') != '')
      & F.col('web_activity').rlike(allowed_activity)
    )
    .drop('_timestamp_count', '_timestamp_micros')
  )


def add_activity_lookup(input_df: DataFrame, lookup_df: DataFrame) -> DataFrame:
  filtered_input = filter_invalid_records(input_df)
  lookup = lookup_df.select(
    F.col('activity').alias('_lookup_activity'),
    F.col('activity_code').cast('int').alias('_lookup_code'),
  )
  tokens = filtered_input.withColumn(
    '_activity_token', F.explode(F.split(F.col('web_activity'), r'\|'))
  )
  matched_tokens = tokens.join(
    lookup,
    tokens['_activity_token'] == lookup['_lookup_activity'],
    'left',
  )

  # Each event is guaranteed to match at most one lookup code.
  return matched_tokens.groupBy(*filtered_input.columns).agg(
    F.coalesce(F.max('_lookup_code'), F.lit(999)).alias('activity_code')
  )


def get_top_3_active_hours(input_df: DataFrame) -> DataFrame:
  filtered_input = filter_invalid_records(input_df).withColumn(
    '_event_time', _epoch_micros_to_timestamp(F.col('timestamp'))
  )
  hourly_counts = (
    filtered_input
    .withColumn('day', F.to_date('_event_time'))
    .withColumn('hour', F.hour('_event_time'))
    .groupBy('day', 'hour')
    .agg(F.count(F.lit(1)).alias('activity_count'))
  )
  # Ties go to the earlier hour.
  daily_rank = Window.partitionBy('day').orderBy(
    F.col('activity_count').desc(), F.col('hour').asc()
  )

  return (
    hourly_counts
    .withColumn('_rank', F.row_number().over(daily_rank))
    .filter(F.col('_rank') <= 3)
    .select('hour', 'day', 'activity_count')
  )


def get_top_2_activity_codes(input_df: DataFrame, lookup_df: DataFrame) -> DataFrame:
  input_with_activity_code = add_activity_lookup(input_df, lookup_df).withColumn(
    '_event_time', _epoch_micros_to_timestamp(F.col('timestamp'))
  )
  daily_counts = (
    input_with_activity_code
    .withColumn('day', F.to_date('_event_time'))
    .groupBy('day', 'activity_code')
    .agg(F.count(F.lit(1)).alias('activity_count'))
  )
  # Ties go to the lower activity code.
  daily_rank = Window.partitionBy('day').orderBy(
    F.col('activity_count').desc(), F.col('activity_code').asc()
  )

  return (
    daily_counts
    .withColumn('_rank', F.row_number().over(daily_rank))
    .filter(F.col('_rank') <= 2)
    .select('activity_code', 'day', 'activity_count')
  )


def get_activity_sessions_per_user(input_df: DataFrame, lookup_df: DataFrame) -> DataFrame:
  input_with_activity_code = add_activity_lookup(input_df, lookup_df).withColumn(
    '_event_micros', F.col('timestamp').cast('long')
  )
  by_user_time = Window.partitionBy('user_id').orderBy('_event_micros')
  session_number_window = by_user_time.rowsBetween(
    Window.unboundedPreceding, Window.currentRow
  )
  gap_micros = 3 * 60 * 60 * 1_000_000

  sessions = (
    input_with_activity_code
    .withColumn('_previous_micros', F.lag('_event_micros').over(by_user_time))
    .withColumn(
      '_new_session',
      F.when(
        F.col('_previous_micros').isNull()
        | ((F.col('_event_micros') - F.col('_previous_micros')) >= gap_micros),
        1,
      ).otherwise(0),
    )
    .withColumn('_session_number', F.sum('_new_session').over(session_number_window))
    .groupBy('user_id', '_session_number')
    .agg(
      F.min('_event_micros').alias('_session_start_micros'),
      F.max('_event_micros').alias('_session_end_micros'),
    )
  )

  # Session lengths are whole minutes, rounded down.
  return sessions.select(
    F.to_date(_epoch_micros_to_timestamp(F.col('_session_start_micros')))
    .alias('session_start_date'),
    'user_id',
    F.floor(
      (F.col('_session_end_micros') - F.col('_session_start_micros'))
      / F.lit(60_000_000)
    ).cast('long').alias('session_time_in_mins'),
  )


# this is just a dummy main function, which doesn't need to be implemented.
def main():
    print("Entering app..")
	# spark_session = SparkSession.builder.appName(APP_NAME).getOrCreate()
	# filter_invalid_records(input_df)

if __name__ == '__main__':
    main()
