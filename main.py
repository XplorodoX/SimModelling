import os
import sys
import time
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


def wait_for_db(engine, retries: int = 10, delay: int = 3) -> None:
    """Waits until a database connection can be established."""
    for _ in range(retries):
        try:
            with engine.connect():
                print("Datenbankverbindung erfolgreich hergestellt.")
                return
        except SQLAlchemyError as exc:
            print(f"Warte auf die Datenbank... ({exc})")
            time.sleep(delay)
    print("Konnte keine Verbindung zur Datenbank herstellen.", file=sys.stderr)
    sys.exit(1)


def import_csv_to_postgres(csv_path: str) -> None:
    """Imports the given CSV file into PostgreSQL using dynamic schema."""
    table_name = os.path.splitext(os.path.basename(csv_path))[0]

    db_user = os.getenv("POSTGRES_USER", "admin")
    db_password = os.getenv("POSTGRES_PASSWORD", "mysecretpassword")
    db_name = os.getenv("POSTGRES_DB", "csv_imports")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")

    db_url = (
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )
    engine = create_engine(db_url)
    wait_for_db(engine)

    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"CSV-Datei nicht gefunden: {csv_path}", file=sys.stderr)
        sys.exit(1)

    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = pd.to_datetime(df[col], errors="ignore")

    # Create table and insert data
    df.head(0).to_sql(table_name, engine, index=False, if_exists="replace")
    df.to_sql(table_name, engine, index=False, if_exists="append", method="multi")
    print(f"Daten erfolgreich in Tabelle '{table_name}' importiert.")


if __name__ == "__main__":
    csv_file = sys.argv[1] if len(sys.argv) > 1 else "sample_csv.csv"
    import_csv_to_postgres(csv_file)
