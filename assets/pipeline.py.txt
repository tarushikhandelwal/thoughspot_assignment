from dagster import asset, job, Output, PartitionedConfig, PartitionsDefinition
import pandas as pd
from dagster_pandas import PandasIOManager
from .resources import sqlite_io_manager
from datetime import datetime

# Partitioning Configuration: Hourly Partition for a Week (168 partitions)
def hourly_partition_fn(date: str) -> str:
    return date

hourly_partitions = PartitionsDefinition(
    name="hourly_partition",
    partition_fn=hourly_partition_fn,
    start="1970-01-01 00:00:00",  # Adjust the start date
    end="1970-01-08 00:00:00",  # Adjust to ensure it covers 7 days (168 hours)
    interval="1H",
)

# Asset 1: Hourly Partitioned Clicks Table
@asset(name="clicks_table", io_manager_key="sqlite_io_manager", partitions_def=hourly_partitions)
def clicks_table(context):
    # Load the clicks data
    clicks_df = pd.read_csv('data/clicks.csv')
    
    # Convert the timestamp columns to datetime
    clicks_df['session_start'] = pd.to_datetime(clicks_df['session_start'])
    clicks_df['click_timestamp'] = pd.to_datetime(clicks_df['click_timestamp'])

    # Partition the data by hour (ensure correct partitioning here)
    clicks_df['hour_partition'] = clicks_df['click_timestamp'].dt.floor('H')

    return clicks_df

# Asset 2: Articles Metadata Table
@asset(name="articles_table", io_manager_key="sqlite_io_manager")
def articles_table(context):
    # Load the articles metadata
    articles_df = pd.read_csv('data/articles_metadata.csv')
    
    # Convert the created_at_ts column to datetime
    articles_df['created_at_ts'] = pd.to_datetime(articles_df['created_at_ts'])

    return articles_df

# Asset 3: Joined Data from Clicks and Articles
@asset(name="joined_data", io_manager_key="sqlite_io_manager")
def joined_data(context, clicks_table, articles_table):
    # Merge the clicks and articles data on 'article_id' and 'click_article_id'
    joined_df = pd.merge(
        clicks_table,
        articles_table,
        left_on='click_article_id',
        right_on='article_id',
        how='inner'
    )
    
    # Drop the duplicate 'article_id' column
    joined_df = joined_df.drop(columns=['article_id'])

    return joined_df

# Asset 4: Daily Partitioned Data (Based on the Join)
@asset(name="daily_partitioned_data", io_manager_key="sqlite_io_manager", partitions_def=PartitionedConfig('daily'))
def daily_partitioned_data(context, joined_data):
    # Partition the joined data by day
    joined_data['day_partition'] = joined_data['click_timestamp'].dt.date

    # Return the daily partitioned data
    return joined_data

# Job: Entire pipeline execution
@job(resource_defs={"sqlite_io_manager": sqlite_io_manager})
def process_data_pipeline():
    clicks = clicks_table()
    articles = articles_table()
    joined = joined_data(clicks, articles)
    daily_part = daily_partitioned_data(joined)
