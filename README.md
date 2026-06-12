markdown# Gas Price ETL Pipeline

## Task
Build an ETL pipeline that extracts data from an API, transforms it, and loads it into a Postgres database.

## Key Points
- Used CollectAPI's Gas Prices API (`stateUsaPrice` endpoint) to extract data.
- Used SQLAlchemy with the psycopg2 adapter to connect to PostgreSQL.
- Used Pandas to clean and rename columns, then loaded the data using `df.to_sql()`.

## My Approach
Built the pipeline in `pipeline.py`, structured into three functions:
- `extract_city_prices()` — fetches gas price data from the API
- `transform_city_prices()` — cleans and renames the data into a DataFrame
- `load_city_prices()` — loads the DataFrame into a Postgres table

All steps run in sequence through `main()`.

## Steps

### 1. Create a virtual environment & activate it
python -m venv venv

venv\Scripts\activate

### 2. Install dependencies
pip install -r requirements.txt

### 3. Add your database credentials and API key

Create a `.env` file in the project root:
DATABASE_NAME=

DATABASE_USER=

DATABASE_PORT=

DATABASE_PASSWORD=

DATABASE_HOST=

COLLECTAPI_KEY=

### 4. Run the pipeline
python pipeline.py

## Output
Successfully Loaded 14 Records in the Db

Successfully returned 14 records using an SQL Query from PG Admin