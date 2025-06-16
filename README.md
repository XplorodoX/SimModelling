# SimModelling CSV Import

This project demonstrates how to import a CSV file into a Postgres database running in Docker.

## Usage

1. Start the database with Docker Compose:

   ```
   docker compose up -d
   ```

2. Install the Python dependencies:

   ```
   pip install -r requirements.txt
   ```

3. Run the importer from your IDE or terminal:

   ```
   python main.py path/to/your.csv
   ```

   If no path is provided, `sample_csv.csv` is used by default.

### Environment variables

The script uses the following variables for the database connection (values in parentheses are defaults):

- `POSTGRES_USER` (`admin`)
- `POSTGRES_PASSWORD` (`mysecretpassword`)
- `POSTGRES_DB` (`csv_imports`)
- `DB_HOST` (`localhost`)
- `DB_PORT` (`5432`)

The table name is derived from the file name, and column names and types are inferred from the CSV automatically.
