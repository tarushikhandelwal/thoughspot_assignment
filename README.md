
# News Portal Data Pipeline

This project is a data pipeline for a news portal that processes user click data and article metadata. It uses **Dagster** to manage the pipeline execution and **SQLite** as the storage backend for storing processed data. The pipeline consists of several assets that transform raw data (CSV files) and load it into a relational database, where the data is further processed.

## Project Structure

- **pipeline.py**: Contains the Dagster pipeline and asset definitions.
- **resources.py**: Defines resources such as the SQLite IO Manager for reading and writing to the database.
- **test_pipeline.py**: Contains unit tests for the pipeline and assets, using **pytest**.
- **dagster.yaml**: The configuration file for the Dagster pipeline.
- **news_portal.db**: The SQLite database that stores the processed data.
- **data/**: A folder containing the CSV files (`clicks.csv` and `articles_metadata.csv`) used for the pipeline.

## Requirements

- Python 3.7+
- **Dagster**: Orchestrates the pipeline and asset execution.
- **SQLite**: Relational database used for storing processed data.
- **pandas**: Used for transforming data.
- **pytest**: Testing framework for unit tests.

You can install the required Python packages using the provided `requirements.txt` file:

```bash
pip install -r requirements.txt
