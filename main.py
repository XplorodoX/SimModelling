import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import time


def map_pandas_dtype_to_sql(dtype):
    """Maps pandas dtype to a corresponding SQL data type."""
    if "int" in str(dtype):
        return "INTEGER"
    elif "float" in str(dtype):
        return "FLOAT"
    elif "datetime" in str(dtype):
        return "TIMESTAMP"
    elif "object" in str(dtype):
        return "TEXT"
    else:
        return "TEXT"


def import_csv_to_postgres(csv_path, table_name):
    """
    Dynamically imports a CSV file into a PostgreSQL table.
    It creates the table based on the CSV structure and then copies the data.
    """
    # --- 1. Datenbankverbindungsinformationen aus Umgebungsvariablen holen ---
    # Diese werden von docker-compose.yml bereitgestellt.
    db_user = os.getenv('POSTGRES_USER', 'user')
    db_password = os.getenv('POSTGRES_PASSWORD', 'password')
    db_name = os.getenv('POSTGRES_DB', 'db')
    db_host = os.getenv('DB_HOST', 'db')  # 'db' ist der Service-Name in docker-compose
    db_port = os.getenv('DB_PORT', '5432')

    # Verbindungs-URL für SQLAlchemy
    db_url = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

    engine = None
    # Versuch, die Verbindung herzustellen (wartet bei Bedarf auf die DB)
    for _ in range(10):  # Versuche es 10 Mal
        try:
            engine = create_engine(db_url)
            with engine.connect() as connection:
                print("Datenbankverbindung erfolgreich hergestellt.")
                break
        except SQLAlchemyError as e:
            print(f"Warte auf die Datenbank... ({e})")
            time.sleep(3)
    else:
        print("Konnte keine Verbindung zur Datenbank herstellen. Skript wird beendet.", file=sys.stderr)
        sys.exit(1)

    # --- 2. CSV einlesen und Schema ableiten ---
    try:
        df = pd.read_csv(csv_path, sep=',')
        # Konvertiere Spalten, die wie Daten aussehen
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    df[col] = pd.to_datetime(df[col], errors='raise')
                except (ValueError, TypeError):
                    pass  # Ist kein Datum, ignoriere es
        print(f"CSV-Datei '{csv_path}' erfolgreich eingelesen.")
    except FileNotFoundError:
        print(f"Fehler: CSV-Datei nicht gefunden unter '{csv_path}'.", file=sys.stderr)
        sys.exit(1)

    # --- 3. SQL CREATE TABLE Statement dynamisch erstellen ---
    columns_sql = []
    for column, dtype in df.dtypes.items():
        sql_type = map_pandas_dtype_to_sql(dtype)
        # Bereinige Spaltennamen für SQL (z.B. keine Leerzeichen)
        clean_column = f'"{column}"'
        columns_sql.append(f"{clean_column} {sql_type}")

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        {', '.join(columns_sql)}
    );
    """

    # --- 4. Tabelle erstellen und Daten importieren ---
    try:
        with engine.begin() as connection:
            print(f"Erstelle Tabelle '{table_name}'...")
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name};"))
            connection.execute(text(create_table_sql))
            print("Tabelle erfolgreich erstellt.")

        # pandas' to_sql für den Datenimport verwenden.
        # 'if_exists='append'' fügt die Daten der gerade erstellten Tabelle hinzu.
        # 'index=False' vermeidet das Schreiben des Pandas-Index in die DB.
        print("Starte Datenimport...")
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Daten erfolgreich in die Tabelle '{table_name}' importiert.")

    except SQLAlchemyError as e:
        print(f"Ein SQL-Fehler ist aufgetreten: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Ein unerwarteter Fehler ist aufgetreten: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    csv_file_path = '/Users/merluee/PycharmProjects/SimModelling/organizations-500000.csv'  # Pfad innerhalb des Containers
    # Tabellenname aus dem Dateinamen ableiten (z.B. ihre_daten.csv -> ihre_daten)
    table_name_from_file = os.path.splitext(os.path.basename(csv_file_path))[0]

    import_csv_to_postgres(csv_file_path, table_name_from_file)
