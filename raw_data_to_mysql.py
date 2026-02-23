import csv
import requests
from datetime import datetime
import mysql.connector

# Conectar a MySQL
conn = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="admin",
    password="admin",
    database="embalses_db"
)
cursor = conn.cursor()

# Crear tabla si no existe
cursor.execute("""
CREATE TABLE IF NOT EXISTS embalses (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fecha DATE,
    embalse VARCHAR(255),
    nivel_absoluto FLOAT,
    porcentaje_volumen_embalsado FLOAT,
    volumen_embalsado FLOAT
)
""")
conn.commit()

# Obtener CSV
url = "https://analisi.transparenciacatalunya.cat/api/views/gn9e-3qhr/rows.csv?accessType=DOWNLOAD"
response = requests.get(url)
response.raise_for_status()

decoded = response.content.decode("utf-8").splitlines()
reader = csv.DictReader(decoded)

insert_query = """
INSERT INTO embalses (fecha, embalse, nivel_absoluto, porcentaje_volumen_embalsado, volumen_embalsado)
VALUES (%s, %s, %s, %s, %s)
"""

rows_to_insert = []
for row in reader:
    # Convertir fecha
    fecha = datetime.strptime(row.get("Dia"), "%d/%m/%Y").date() if row.get("Dia") else None

    # Leer columnas exactas
    embalse = row.get("Estació")
    nivel_absoluto = float(row.get("Nivell absolut (msnm)")) if row.get("Nivell absolut (msnm)") else None
    porcentaje_volumen_embalsado = float(row.get("Percentatge volum embassat (%)")) if row.get("Percentatge volum embassat (%)") else None
    volumen_embalsado = float(row.get("Volum embassat (hm3)")) if row.get("Volum embassat (hm3)") else None

    rows_to_insert.append((fecha, embalse, nivel_absoluto, porcentaje_volumen_embalsado, volumen_embalsado))

if rows_to_insert:
    cursor.executemany(insert_query, rows_to_insert)
    conn.commit()
    print(f"Filas insertadas: {cursor.rowcount}")
else:
    print("No se han insertado filas.")

cursor.close()
conn.close()
