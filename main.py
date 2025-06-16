import os
import sys
import time
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


def wait_for_db(engine, retries: int = 10, delay: int = 3) -> None:
    """Waits until a database connection can be established."""
    for _ in range(retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                print("Datenbankverbindung erfolgreich hergestellt.")
                return
        except SQLAlchemyError as exc:
            print(f"Warte auf die Datenbank... ({exc})")
            time.sleep(delay)
    print("Konnte keine Verbindung zur Datenbank herstellen.", file=sys.stderr)
    sys.exit(1)


def import_csv_to_postgres(csv_path: str) -> None:
    """Imports the given CSV file into PostgreSQL using dynamic schema."""

    # Prüfen ob CSV-Datei existiert
    if not os.path.exists(csv_path):
        print(f"CSV-Datei nicht gefunden: {csv_path}", file=sys.stderr)
        sys.exit(1)

    table_name = os.path.splitext(os.path.basename(csv_path))[0]
    # Tabellennamen bereinigen (nur alphanumerisch und Unterstriche)
    table_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in table_name)

    db_user = os.getenv("POSTGRES_USER", "admin")
    db_password = os.getenv("POSTGRES_PASSWORD", "mysecretpassword")
    db_name = os.getenv("POSTGRES_DB", "csv_imports")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")

    db_url = (
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    try:
        engine = create_engine(db_url, echo=True)  # echo=True für Debug-Ausgaben
        wait_for_db(engine)

        # CSV lesen mit besserer Fehlerbehandlung
        print(f"Lade CSV-Datei: {csv_path}")
        try:
            df = pd.read_csv(csv_path, encoding='utf-8')
        except UnicodeDecodeError:
            # Fallback für andere Encodings
            df = pd.read_csv(csv_path, encoding='latin-1')

        print(f"CSV geladen. Shape: {df.shape}")
        print(f"Spalten: {list(df.columns)}")
        print(f"Erste 3 Zeilen:\n{df.head(3)}")

        # Datentypen optimieren
        # Versuche automatische Datum-Konvertierung nur für wahrscheinliche Datumsspalten
        date_columns = []
        for col in df.select_dtypes(include=["object"]).columns:
            # Nur Spalten mit "date", "time" im Namen oder wenige eindeutige Werte prüfen
            if any(keyword in col.lower() for keyword in ['date', 'time', 'datum']) or df[col].nunique() < len(
                    df) * 0.1:
                try:
                    temp_series = pd.to_datetime(df[col], errors='coerce')
                    # Wenn mehr als 50% der Werte erfolgreich konvertiert wurden
                    if temp_series.notna().sum() > len(df) * 0.5:
                        df[col] = temp_series
                        date_columns.append(col)
                        print(f"Spalte '{col}' als Datum erkannt und konvertiert")
                except:
                    pass

        # Spaltennamen bereinigen
        df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

        with engine.connect() as conn:
            # Tabelle löschen falls sie existiert
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            conn.commit()
            print(f"Bestehende Tabelle '{table_name}' gelöscht")

            # Daten in einem Schritt einfügen
            rows_inserted = df.to_sql(
                table_name,
                conn,
                index=False,
                if_exists="replace",
                method="multi",
                chunksize=1000
            )

            conn.commit()
            print(f"✓ {len(df)} Zeilen erfolgreich in Tabelle '{table_name}' importiert.")

            # Verifikation: Anzahl Zeilen in der Tabelle prüfen
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.fetchone()[0]
            print(f"✓ Verifikation: {count} Zeilen in der Datenbank-Tabelle")

            # Erste paar Zeilen aus der DB anzeigen
            result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 3"))
            rows = result.fetchall()
            if rows:
                print("Erste 3 Zeilen aus der Datenbank:")
                for row in rows:
                    print(row)

    except SQLAlchemyError as e:
        print(f"Datenbankfehler: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Allgemeiner Fehler: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Verwendung: python main.py <csv_datei>")
        print("Beispiel: python main.py sample_csv.csv")
        sys.exit(1)

    csv_file = sys.argv[1]
    import_csv_to_postgres(csv_file)