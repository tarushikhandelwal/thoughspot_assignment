import pytest
import pandas as pd
from dagster import build_asset_context
from pipeline import clicks_table, articles_table, joined_data, daily_partitioned_data
from resources import sqlite_io_manager


# Mocking the database for tests
@pytest.fixture
def mock_db():
    # Create a temporary SQLite database for testing purposes
    from sqlite3 import connect
    conn = connect(':memory:')  # In-memory database for testing
    yield conn
    conn.close()

# Test the clicks_table asset
def test_clicks_table(mock_db):
    # Sample data for clicks.csv
    clicks_data = {
        'user_id': [1, 2],
        'session_id': [1001, 1002],
        'session_start': ['2025-01-01 10:00:00', '2025-01-01 11:00:00'],
        'session_size': [3, 4],
        'click_article_id': [101, 102],
        'click_timestamp': ['2025-01-01 10:05:00', '2025-01-01 11:10:00'],
        'click_environment': ['web', 'mobile'],
        'click_deviceGroup': ['smartphone', 'tablet'],
        'click_os': ['Android', 'iOS'],
        'click_country': ['US', 'IN'],
        'click_region': ['CA', 'NY'],
        'click_referrer_type': ['search', 'social']
    }

    clicks_df = pd.DataFrame(clicks_data)
    clicks_df['session_start'] = pd.to_datetime(clicks_df['session_start'])
    clicks_df['click_timestamp'] = pd.to_datetime(clicks_df['click_timestamp'])

    # Simulate the context for the asset
    context = build_asset_context(asset_key="clicks_table", resources={"sqlite_io_manager": sqlite_io_manager(mock_db)})

    # Write data to the SQLite DB
    clicks_table(context)

    # Read the data from the database to verify
    result_df = pd.read_sql("SELECT * FROM clicks_table", mock_db)
    assert not result_df.empty
    assert set(result_df.columns) == {"user_id", "session_id", "session_start", "session_size",
                                      "click_article_id", "click_timestamp", "click_environment",
                                      "click_deviceGroup", "click_os", "click_country", "click_region", "click_referrer_type"}


# Test the articles_table asset
def test_articles_table(mock_db):
    # Sample data for articles_metadata.csv
    articles_data = {
        'article_id': [101, 102],
        'category_id': [1, 2],
        'created_at_ts': ['2025-01-01 09:00:00', '2025-01-01 09:30:00'],
        'publisher_id': [501, 502],
        'words_count': [1000, 1500]
    }

    articles_df = pd.DataFrame(articles_data)
    articles_df['created_at_ts'] = pd.to_datetime(articles_df['created_at_ts'])

    # Simulate the context for the asset
    context = build_asset_context(asset_key="articles_table", resources={"sqlite_io_manager": sqlite_io_manager(mock_db)})

    # Write data to the SQLite DB
    articles_table(context)

    # Read the data from the database to verify
    result_df = pd.read_sql("SELECT * FROM articles_table", mock_db)
    assert not result_df.empty
    assert set(result_df.columns) == {"article_id", "category_id", "created_at_ts", "publisher_id", "words_count"}


# Test the joined_data asset
def test_joined_data(mock_db):
    # Sample data for clicks.csv
    clicks_data = {
        'user_id': [1, 2],
        'session_id': [1001, 1002],
        'session_start': ['2025-01-01 10:00:00', '2025-01-01 11:00:00'],
        'session_size': [3, 4],
        'click_article_id': [101, 102],
        'click_timestamp': ['2025-01-01 10:05:00', '2025-01-01 11:10:00'],
        'click_environment': ['web', 'mobile'],
        'click_deviceGroup': ['smartphone', 'tablet'],
        'click_os': ['Android', 'iOS'],
        'click_country': ['US', 'IN'],
        'click_region': ['CA', 'NY'],
        'click_referrer_type': ['search', 'social']
    }

    clicks_df = pd.DataFrame(clicks_data)
    clicks_df['session_start'] = pd.to_datetime(clicks_df['session_start'])
    clicks_df['click_timestamp'] = pd.to_datetime(clicks_df['click_timestamp'])

    # Sample data for articles_metadata.csv
    articles_data = {
        'article_id': [101, 102],
        'category_id': [1, 2],
        'created_at_ts': ['2025-01-01 09:00:00', '2025-01-01 09:30:00'],
        'publisher_id': [501, 502],
        'words_count': [1000, 1500]
    }

    articles_df = pd.DataFrame(articles_data)
    articles_df['created_at_ts'] = pd.to_datetime(articles_df['created_at_ts'])

    # Write the data to the SQLite DB using the appropriate assets
    context_clicks = build_asset_context(asset_key="clicks_table", resources={"sqlite_io_manager": sqlite_io_manager(mock_db)})
    context_articles = build_asset_context(asset_key="articles_table", resources={"sqlite_io_manager": sqlite_io_manager(mock_db)})
    
    # Write data to the SQLite DB
    clicks_table(context_clicks)
    articles_table(context_articles)

    # Test the join asset
    context_joined = build_asset_context(asset_key="joined_data", resources={"sqlite_io_manager": sqlite_io_manager(mock_db)})
    joined_df = joined_data(context_joined, clicks_df, articles_df)

    assert not joined_df.empty
    assert set(joined_df.columns) == {"user_id", "session_id", "session_start", "session_size",
                                      "click_article_id", "click_timestamp", "click_environment",
                                      "click_deviceGroup", "click_os", "click_country", "click_region",
                                      "click_referrer_type", "category_id", "created_at_ts", "publisher_id", "words_count"}


# Test the daily_partitioned_data asset
def test_daily_partitioned_data(mock_db):
    # Simulate data for daily partitioning test
    data = {
        'user_id': [1, 2],
        'session_id': [1001, 1002],
        'click_article_id': [101, 102],
        'click_timestamp': ['2025-01-01 10:05:00', '2025-01-01 11:10:00'],
        'category_id': [1, 2],
        'created_at_ts': ['2025-01-01 09:00:00', '2025-01-01 09:30:00'],
        'words_count': [1000, 1500]
    }
    df = pd.DataFrame(data)
    df['click_timestamp'] = pd.to_datetime(df['click_timestamp'])
    df['created_at_ts'] = pd.to_datetime(df['created_at_ts'])

    # Simulate the context for the asset
    context = build_asset_context(asset_key="daily_partitioned_data", resources={"sqlite_io_manager": sqlite_io_manager(mock_db)})

    # Process the data
    daily_partitioned_df = daily_partitioned_data(context, df)

    assert not daily_partitioned_df.empty
    assert 'day_partition' in daily_partitioned_df.columns  # Check if the data was partitioned by day
    assert daily_partitioned_df['day_partition'].nunique() == 1  # In this test case, all data should be from one day

