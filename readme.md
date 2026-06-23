# News API ETL Pipeline

A Python ETL pipeline that pulls news articles from NewsAPI and stores them in a PostgreSQL database.

---

## Project Structure

```
├── extract.py      # Fetch articles from NewsAPI
├── transform.py    # Clean the data
├── load.py         # Save data to PostgreSQL
├── main.py         # Run the pipeline
└── requirement.txt # Dependencies
```

---

## Setup

**1. Install dependencies**
```bash
pip install -r requirement.txt
```

**2. Create a `.env` file**
```
NEWS_API_KEY=your_key_here
DATABASE_NAME=your_db
DATABASE_USER=your_user
DATABASE_PASSWORD=your_password
DATABASE_HOST=localhost
DATABASE_PORT=5432
```

**3. Run the pipeline**
```bash
python main.py
```

---

## What it does

- Fetches Apple-related news articles from NewsAPI
- Cleans the data using pandas
- Loads the results into a PostgreSQL table called `articles`