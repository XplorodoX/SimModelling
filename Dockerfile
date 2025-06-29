# Starten mit einem schlanken Python-Basisimage
FROM python:3.9-slim

# Arbeitsverzeichnis im Container festlegen
WORKDIR /app

# requirements.txt kopieren, um Abhängigkeiten zu installieren
COPY requirements.txt .

# Python-Bibliotheken installieren
RUN pip install --no-cache-dir -r requirements.txt

# Den Rest des Codes in das Arbeitsverzeichnis kopieren
COPY . .

# Befehl, der beim Start des Containers ausgeführt wird
CMD ["python", "main.py"]
