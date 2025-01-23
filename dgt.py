import os
import sqlite3
import requests
from zipfile import ZipFile
from datetime import datetime
import argparse

# Constantes para URL y valores predeterminados
BASE_URL = "https://www.dgt.es/microdatos/salida/{year}/{month}/vehiculos/matriculaciones/export_mensual_mat_{year}{month:02d}.zip"
DEFAULT_START_YEAR = 2014
DEFAULT_START_MONTH = 12
DATA_FOLDER = "dgt_data"
DB_PATH = "matriculaciones.db"

# Crear carpeta para almacenar archivos descargados
os.makedirs(DATA_FOLDER, exist_ok=True)

# Estructura para campos, tamaños y tipos
FIELDS = {
    "fecha_matriculacion":         ( 8, "DATE"),
    "clase_matricula":             ( 1, "TEXT"),
    "fecha_transferencia":         ( 8, "DATE"),
    "vehiculo_marca":              (30, "TEXT"),
    "vehiculo_modelo":             (22, "TEXT"),
    "codigo_procedencia":          ( 1, "TEXT"),
    "bastidor":                    (21, "TEXT"),
    "codigo_tipo":                 ( 2, "TEXT"),
    "cod_propulsion":              ( 1, "TEXT"),
    "cilindrada":                  ( 5, "REAL"),
    "potencia":                    ( 6, "REAL"),
    "tara":                        ( 6, "REAL"),
    "peso_maximo":                 ( 6, "REAL"),
    "plazas":                      ( 3, "INTEGER"),
    "precintado":                  ( 2, "INTEGER"),
    "embargado":                   ( 2, "INTEGER"),
    "transmisiones":               ( 2, "INTEGER"),
    "titulares":                   ( 2, "INTEGER"),
    "localidad":                   (24, "TEXT"),
    "provincia":                   ( 2, "TEXT"),
    "provincia_matriculacion":     ( 2, "TEXT"),
    "tramite":                     ( 1, "TEXT"),
    "fecha_tramite":               ( 8, "DATE"),
    "codigo_postal":               ( 5, "TEXT"),
    "fecha_primera_matriculacion": ( 8, "DATE"),
    "nuevo":                       ( 1, "INTEGER"),
    "persona_juridica":            ( 1, "INTEGER"),
    "codigo_itv":                  ( 9, "TEXT"),
    "servicio":                    ( 3, "TEXT"),
    "codigo_municipio_ine":        ( 5, "INTEGER"),
    "municipio":                   (30, "TEXT"),
    "potencia_kw":                 ( 7, "REAL"),
    "plazas_maximo":               ( 3, "INTEGER"),
    "co2":                         ( 5, "INTEGER"),
    "renting":                     ( 1, "INTEGER"),
    "titular_tutelado":            ( 1, "INTEGER"),
}

def create_table(cursor):
    fields_sql = ",\n    ".join(f"{field} {details[1]}" for field, details in FIELDS.items())
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS matriculaciones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {fields_sql},
            UNIQUE (bastidor, fecha_matriculacion)
        )
    ''')

def parse_line(line):
    """Parses a line from the TXT file based on the FIELDS sizes."""
    def parse_value(value, field_name, field_type):
        if field_type == "DATE":
            try:
                return datetime.strptime(value.strip(), "%d%m%Y").date() if value.strip() else None
            except ValueError:
                return None
        elif field_type == "REAL":
            try:
                return float(value.strip()) if value.strip() else None
            except ValueError:
                return None
        elif field_type == "INTEGER":
            if field_name in ["nuevo", "persona_juridica"]:
                return 1 if value.strip() in ["N", "X"] else 0
            if field_name in ["precintado", "embargado", "renting", "titular_tutelado"]:
                return 1 if value.strip() in ["SI", "S"] else 0
            try:
                return int(value.strip()) if value.strip() else None
            except ValueError:
                return None
        else:  # TEXT
            return value.strip()

    parsed = {}
    index = 0
    for field, (size, field_type) in FIELDS.items():
        raw_value = line[index:index + size]
        index += size
        parsed[field] = parse_value(raw_value, field, field_type)

    return parsed

def download_and_extract_file(year, month):
    # Construcción de la URL y el nombre del ZIP
    zip_url = BASE_URL.format(year=year, month=month)
    zip_name = os.path.join(DATA_FOLDER, f"export_mensual_mat_{year}{month:02d}.zip")

    try:
        # Descargar el archivo ZIP
        print(f"Descargando: {zip_url}")
        response = requests.get(zip_url)
        response.raise_for_status()

        # Guardar el archivo ZIP
        with open(zip_name, "wb") as zip_file:
            zip_file.write(response.content)

        # Descomprimir el archivo, eliminando estructuras de carpetas
        extracted_files = []
        with ZipFile(zip_name, "r") as zip_ref:
            for member in zip_ref.namelist():
                filename = os.path.basename(member)
                if filename:  # Evita entradas que sean solo carpetas
                    source = zip_ref.open(member)
                    target_path = os.path.join(DATA_FOLDER, filename)
                    with open(target_path, "wb") as target:
                        target.write(source.read())
                    extracted_files.append(target_path)

        print(f"Archivo descargado y extraído: {zip_name}")
        return extracted_files[0] if extracted_files else None
    except requests.exceptions.RequestException as e:
        print(f"Error al descargar {zip_url}: {e}")
    except Exception as e:
        print(f"Error al procesar {zip_name}: {e}")

def process_and_insert_file(file_path, cursor):
    print(f"Procesando: {file_path}")
    with open(file_path, "r", encoding="latin-1") as txt_file:
        for line in txt_file:
            if line.strip() and not line.startswith("Vehículos matriculados"):
                record = parse_line(line)
                try:
                    cursor.execute(f'''
                        INSERT OR IGNORE INTO matriculaciones ({', '.join(FIELDS.keys())})
                        VALUES ({', '.join(['?' for _ in FIELDS])})
                    ''', tuple(record[field] for field in FIELDS))
                except sqlite3.IntegrityError as e:
                    print(f"Error al insertar registro: {e}")

def main(start_year=DEFAULT_START_YEAR, start_month=DEFAULT_START_MONTH, end_year=None, end_month=None):
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    cursor = conn.cursor()

    create_table(cursor)

    end_year = end_year or datetime.now().year
    end_month = end_month or datetime.now().month

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == start_year and month < start_month:
                continue
            if year == end_year and month > end_month:
                break

            txt_file_path = download_and_extract_file(year, month)
            if txt_file_path and os.path.exists(txt_file_path):
                process_and_insert_file(txt_file_path, cursor)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Procesar matriculaciones de la DGT.")
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR, help="Año de inicio (por defecto 2014)")
    parser.add_argument("--start-month", type=int, default=DEFAULT_START_MONTH, help="Mes de inicio (por defecto diciembre)")
    parser.add_argument("--end-year", type=int, help="Año de fin (por defecto el actual)")
    parser.add_argument("--end-month", type=int, help="Mes de fin (por defecto el actual)")

    args = parser.parse_args()

    main(
        start_year=args.start_year,
        start_month=args.start_month,
        end_year=args.end_year,
        end_month=args.end_month
    )