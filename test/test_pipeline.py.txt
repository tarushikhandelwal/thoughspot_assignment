import pytest
from dagster import execute_pipeline
from pipeline import my_pipeline  # Replace with your actual pipeline name
from resources import sqlite_io_manager


# Test to check the pipeline execution end-to-end
def test_pipeline_execution(mock_db):
    # Simulate the resources needed for the pipeline
    resources = {
        "sqlite_io_manager": sqlite_io_manager(mock_db),
    }
    
    # Execute the pipeline
    result = execute_pipeline(my_pipeline, resources=resources)

    # Check if the pipeline executed successfully
    assert result.success

    # Verify that the data has been written to the database (sqlite)
    import sqlite3
    conn = sqlite3.connect(mock_db)
    cursor = conn.cursor()

    # Check if clicks_table exists in the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='clicks_table';")
    table_exists = cursor.fetchone()
    assert table_exists is not None, "clicks_table should exist in the database."

    # Check if articles_table exists in the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='articles_table';")
    table_exists = cursor.fetchone()
    assert table_exists is not None, "articles_table should exist in the database."

    # Check if joined_data table exists in the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='joined_data';")
    table_exists = cursor.fetchone()
    assert table_exists is not None, "joined_data table should exist in the database."

    conn.close()


# Test to verify specific asset transformation (e.g., checking the content of the tables after transformation)
def test_clicks_table_transformation(mock_db):
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
    
    # Test if the DataFrame from DB is as expected
    assert result_df.shape == (2, 12)  # We have 2 rows and 12 columns in the test data
    assert 'user_id' in result_df.columns
    assert 'session_id' in result_df.columns
    assert 'click_article_id' in result_df.columns


# Test for the daily partitioned data transformation
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

    # Test that the partition column is added
    assert 'day_partition' in daily_partitioned_df.columns
    assert daily_partitioned_df['day_partition'].nunique() == 1  # In this test case, all data should be from one day


# Test the resource configuration
def test_sqlite_io_manager(mock_db):
    # Test if the SQLiteIOManager can handle basic output and input operations
    
    # Prepare data to test
    test_data = pd.DataFrame({
        'user_id': [1, 2],
        'session_id': [1001, 1002],
        'click_article_id': [101, 102]
    })
    
    # Set up the resource (SQLite manager)
    io_manager = sqlite_io_manager(mock_db)

    # Writing to DB
    io_manager.handle_output(None, test_data)

    # Read from the database and validate
    read_data = io_manager.load_input(None)
    
    assert read_data.shape == (2, 3)  # Ensure that the data read from DB is as expected
    assert 'user_id' in read
