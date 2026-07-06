# Gas Price ETL Pipeline

## Task
Build an ETL pipeline that extracts gas price data from an API, transforms it, and loads it into a PostgreSQL database. The pipeline is automated using Apache Airflow with two DAG implementations.


## Key Points
- Used CollectAPI's Gas Prices API (`stateUsaPrice` endpoint) to extract data for Washington state cities.
- Used SQLAlchemy with the psycopg2 adapter to connect to PostgreSQL.
- Used Pandas to clean and rename columns, then loaded the data using `df.to_sql()`.
- Automated the pipeline using Apache Airflow — built two DAGs, one using the traditional `PythonOperator` approach and one using the modern `TaskFlow API` decorator style.

## My Approach

### Standalone Pipeline (`pipeline.py`)
Structured into three functions:
- `extract_city_prices()` — fetches gas price data from CollectAPI
- `transform_city_prices()` — cleans and renames the data into a DataFrame
- `load_city_prices()` — loads the DataFrame into a Postgres table

All steps run in sequence through `main()`.

### Airflow DAGs (`dags/`)
Built two versions of the same pipeline as Airflow DAGs:
- `gas_prices_etl_dag.py` — uses `PythonOperator` and manually handles XCom for passing data between tasks
- `gas_prices_taskflow_dag.py` — uses `@dag` and `@task` decorators from the TaskFlow API, XCom is handled automatically

Both DAGs run on a 1 hour schedule.

## Setup (WSL/Ubuntu)

### 1. Clone the repo
```bash
git clone https://github.com/Daboz-cell/Gas_price_ETL.git
cd Gas_price_ETL
```

### 2. Create a virtual environment and activate it
```bash
python3 -m venv your_venv_name
source your_venv_name/bin/activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Create your `.env` file
```bash
DATABASE_NAME=your_db_name
DATABASE_USER=your_db_user
DATABASE_PORT=5432
DATABASE_PASSWORD=your_db_password
DATABASE_HOST=localhost
COLLECTAPI_KEY=apikey your_key_here
```

### 5. Create the database in Ubuntu PostgreSQL
```bash
sudo -u postgres psql -c "CREATE DATABASE your_db_name;"
```

### 6. Run the standalone pipeline
```bash
python pipeline.py
```

### 7. Run via Airflow
Copy the DAG files to your Airflow dags folder:
```bash
cp dags/*.py ~/airflow/dags/
```
Then start Airflow and trigger the DAGs:
```bash
airflow standalone
```

## Output
- 14 records loaded into the `city_prices` table in PostgreSQL
- Data includes gasoline, mid-grade, premium, and diesel prices for 14 Washington state cities