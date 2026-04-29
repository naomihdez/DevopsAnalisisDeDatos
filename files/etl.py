import pandas as pd
import logging
import os
import glob
from datetime import datetime
from sqlalchemy import create_engine, text

# ========================= LOGGING =========================
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/etl.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ========================= SCHEMA MAPPING =========================
# Agrega aquí los nombres alternativos de cada columna para nuevos datasets.
# El sistema detectará automáticamente cuál existe en cada archivo.
SCHEMA = {
    "Order ID": [
        "Order ID", "order_id", "order_number", "id_orden",
        "OrderID", "order", "id", "sale_id", "transaction_id",
    ],
    "Date": [
        "Date", "date", "sale_date", "fecha", "fecha_venta",
        "order_date", "transaction_date", "created_at", "OrderDate",
    ],
    "Amount": [
        "Amount", "amount", "revenue", "total", "monto",
        "sale_amount", "price", "Total", "Sales", "gross_revenue",
        "TotalPrice", "total_price", "total_sale",
    ],
    "Qty": [
        "Qty", "qty", "quantity", "cantidad", "units",
        "Quantity", "items", "num_items", "volume",
    ],
    "Category": [
        "Category", "category", "product_type", "categoria",
        "ProductCategory", "type", "segment", "department",
        "Product", "product", "product_name", "item",
    ],
    "ship-state": [
        "ship-state", "ship_state", "state", "estado", "region",
        "Region", "shipping_state", "delivery_state", "location",
        "StoreLocation", "store_location", "city",
    ],
    "Courier Status": [
        "Courier Status", "courier_status", "status", "estado_envio",
        "delivery_status", "shipping_status", "order_status", "Status",
        "PaymentMethod", "payment_method",
    ],
}


def detect_and_map_schema(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """
    Detecta automáticamente las columnas del DataFrame y las mapea
    al esquema estándar usando el diccionario SCHEMA.
    Retorna el DataFrame con columnas renombradas, o None si faltan columnas críticas.
    """
    mapping = {}
    missing = []

    for standard_col, aliases in SCHEMA.items():
        found = None
        for alias in aliases:
            if alias in df.columns:
                found = alias
                break
        if found:
            mapping[found] = standard_col
        else:
            missing.append(standard_col)

    # Columnas críticas sin las que no podemos procesar
    critical = {"Order ID", "Date", "Amount"}
    critical_missing = critical & set(missing)

    if critical_missing:
        logger.error(
            f"[{filename}] No se puede procesar — columnas críticas faltantes: {critical_missing}"
        )
        logger.error(
            f"[{filename}] Columnas disponibles: {list(df.columns)}"
        )
        logger.error(
            f"[{filename}] Agrega los nombres de esas columnas al diccionario SCHEMA en etl.py"
        )
        return None

    if missing:
        logger.warning(
            f"[{filename}] Columnas opcionales no encontradas (se usará 'Unknown'): {missing}"
        )

    df = df.rename(columns=mapping)

    # Rellenar columnas opcionales que no existían
    for col in ["Category", "ship-state", "Courier Status"]:
        if col not in df.columns:
            df[col] = "Unknown"

    return df


# ========================= EXTRACCIÓN MULTI-ARCHIVO =========================

# ========================= CONEXIÓN MySQL =========================
def get_mysql_engine():
    host     = os.getenv("MYSQL_HOST", "localhost")
    user     = os.getenv("MYSQL_USER", "sales_user")
    password = os.getenv("MYSQL_PASSWORD", "sales_password")
    database = os.getenv("MYSQL_DATABASE", "sales_db")
    url = f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"
    return create_engine(url)

SUPPORTED_EXTENSIONS = [".csv", ".xlsx", ".xls"]


def read_file(filepath: str) -> pd.DataFrame:
    """Lee un archivo CSV, XLSX o XLS y retorna un DataFrame."""
    ext = os.path.splitext(filepath)[1].lower()
    filename = os.path.basename(filepath)

    if ext == ".csv":
        return pd.read_csv(
            filepath,
            encoding="latin1",
            on_bad_lines="skip",
            engine="python",
        )
    elif ext in (".xlsx", ".xls"):
        # Lee la primera hoja por defecto
        df = pd.read_excel(filepath, sheet_name=0, engine="openpyxl" if ext == ".xlsx" else "xlrd")
        logger.info(f"[{filename}] Hojas disponibles (leyendo hoja 0)")
        return df
    else:
        raise ValueError(f"Formato no soportado: {ext}")


def extract_all(data_dir: str = "data"):
    """
    Lee todos los archivos CSV, XLSX y XLS de la carpeta data/.
    Retorna lista de tuplas (DataFrame, nombre_archivo).
    """
    files = []
    for ext in SUPPORTED_EXTENSIONS:
        pattern = os.path.join(data_dir, f"*{ext}")
        found = [
            f for f in glob.glob(pattern)
            if "clean_sales" not in os.path.basename(f)
        ]
        files.extend(found)

    if not files:
        logger.error(f"No se encontraron archivos en {data_dir}/ (soportados: {SUPPORTED_EXTENSIONS})")
        return []

    logger.info(f"Archivos encontrados: {[os.path.basename(f) for f in files]}")

    results = []
    for filepath in files:
        filename = os.path.basename(filepath)
        try:
            df = read_file(filepath)
            logger.info(f"[{filename}] Leídos {len(df)} registros brutos")
            results.append((df, filename))
        except Exception as e:
            logger.error(f"[{filename}] Error al leer: {e}")

    return results


# ========================= TRANSFORMACIÓN =========================
def transform(df: pd.DataFrame, filename: str = "unknown") -> pd.DataFrame:
    """Limpia y transforma un DataFrame ya mapeado al esquema estándar."""
    logger.info(f"[{filename}] Iniciando transformación...")
    initial = len(df)

    # Eliminar columnas basura
    unnamed = [c for c in df.columns if c.startswith("Unnamed")]
    df = df.drop(columns=unnamed, errors="ignore")

    # Tipos y limpieza
    df["Date"]   = pd.to_datetime(df["Date"],   errors="coerce")
    df["Qty"]    = pd.to_numeric(df["Qty"],      errors="coerce")
    df["Amount"] = pd.to_numeric(df["Amount"],   errors="coerce")

    df = df.dropna(subset=["Order ID", "Date", "Amount"])
    df = df[(df["Qty"] > 0) & (df["Amount"] > 0)]

    # Feature engineering
    df["Year"]          = df["Date"].dt.year
    df["Month"]         = df["Date"].dt.month
    df["Price_per_unit"] = df["Amount"] / df["Qty"]

    df["Category"]       = df["Category"].fillna("Unknown")
    df["ship-state"]     = df["ship-state"].fillna("Unknown")
    df["Courier Status"] = df["Courier Status"].fillna("Unknown")

    # Columna de trazabilidad
    df["source_file"] = filename

    removed = initial - len(df)
    logger.info(
        f"[{filename}] Transformación lista: {len(df)} válidos ({removed} eliminados)"
    )
    return df


# ========================= VALIDACIÓN DE CALIDAD =========================
def validate_quality(df: pd.DataFrame) -> dict:
    logger.info("Validando calidad del dataset combinado...")

    # Completitud solo sobre columnas criticas (ignora columnas extra del dataset)
    cols_criticas = ["Order ID", "Date", "Amount", "Qty", "Category", "ship-state", "Courier Status"]
    cols_presentes = [c for c in cols_criticas if c in df.columns]
    completitud  = df[cols_presentes].notnull().mean().mean() * 100
    exactitud    = ((df["Amount"] > 0) & (df["Qty"] > 0)).mean() * 100
    consistencia = (df["Price_per_unit"] > 0).mean() * 100
    quality_score = (completitud + exactitud + consistencia) / 3

    metrics = {
        "completitud":   round(completitud,   2),
        "exactitud":     round(exactitud,     2),
        "consistencia":  round(consistencia,  2),
        "quality_score": round(quality_score, 2),
    }

    logger.info(f"  Completitud  : {completitud:.2f}%  (meta: >95%)")
    logger.info(f"  Exactitud    : {exactitud:.2f}%   (meta: >98%)")
    logger.info(f"  Consistencia : {consistencia:.2f}%  (meta: >97%)")
    logger.info(f"  Score total  : {quality_score:.2f}%")

    if completitud  < 95:
        logger.warning(f"ALERTA: Completitud {completitud:.2f}% < 95%")
    if exactitud    < 98:
        logger.warning(f"ALERTA: Exactitud {exactitud:.2f}% < 98%")
    if consistencia < 97:
        logger.warning(f"ALERTA: Consistencia {consistencia:.2f}% < 97%")

    return metrics


# ========================= CARGA A MySQL =========================
def load(df: pd.DataFrame, engine) -> int:
    logger.info("Cargando datos combinados a MySQL...")

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sales (
                id              INT AUTO_INCREMENT PRIMARY KEY,
                order_id        VARCHAR(100),
                date            DATE,
                amount          DECIMAL(10,2),
                qty             INT,
                category        VARCHAR(100),
                ship_state      VARCHAR(100),
                courier_status  VARCHAR(100),
                year            INT,
                month           INT,
                price_per_unit  DECIMAL(10,2),
                source_file     VARCHAR(255),
                loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.execute(text("TRUNCATE TABLE sales"))

    df_load = df.rename(columns={
        "Order ID":       "order_id",
        "Date":           "date",
        "Amount":         "amount",
        "Qty":            "qty",
        "Category":       "category",
        "ship-state":     "ship_state",
        "Courier Status": "courier_status",
        "Year":           "year",
        "Month":          "month",
        "Price_per_unit": "price_per_unit",
    })

    cols = ["order_id", "date", "amount", "qty", "category",
            "ship_state", "courier_status", "year", "month",
            "price_per_unit", "source_file"]

    df_load[cols].to_sql(
        "sales", engine, if_exists="append", index=False, chunksize=1000
    )

    logger.info(f"Cargados {len(df_load)} registros a MySQL")
    return len(df_load)


# ========================= PIPELINE COMPLETO =========================
def run_etl() -> dict:
    logger.info("=" * 50)
    logger.info("INICIO ETL MULTI-DATASET")
    logger.info("=" * 50)
    start = datetime.now()

    os.makedirs("data", exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    # 1. Leer todos los CSVs
    raw_files = extract_all("data")
    if not raw_files:
        raise FileNotFoundError("No hay archivos CSV en data/")

    total_raw = sum(len(df) for df, _ in raw_files)

    # 2. Mapear esquema + transformar cada archivo
    frames = []
    skipped = []
    for df_raw, filename in raw_files:
        df_mapped = detect_and_map_schema(df_raw, filename)
        if df_mapped is None:
            skipped.append(filename)
            continue
        df_clean = transform(df_mapped, filename)
        frames.append(df_clean)
        logger.info(f"[{filename}] OK — {len(df_clean)} registros listos")

    if not frames:
        raise ValueError("Ningún archivo pudo procesarse. Revisa el diccionario SCHEMA.")

    # 3. Combinar todos los datasets
    df_combined = pd.concat(frames, ignore_index=True)
    logger.info(
        f"Dataset combinado: {len(df_combined)} registros de {len(frames)} archivo(s)"
    )
    if skipped:
        logger.warning(f"Archivos omitidos por esquema incompatible: {skipped}")

    # 4. Validar calidad del dataset combinado
    metrics = validate_quality(df_combined)

    # 5. Guardar CSV limpio
    df_combined.to_csv("data/clean_sales.csv", index=False)
    logger.info("CSV limpio guardado: data/clean_sales.csv")

    # 6. Carga a MySQL
    records_loaded = 0
    try:
        engine = get_mysql_engine()
        records_loaded = load(df_combined, engine)
    except Exception as e:
        logger.warning(f"MySQL no disponible, solo CSV: {e}")

    # 7. Reporte
    duration = (datetime.now() - start).total_seconds()
    os.makedirs("data/reports", exist_ok=True)
    report = f"""
====================================
REPORTE ETL MULTI-DATASET - {datetime.now().strftime('%Y-%m-%d %H:%M')}
====================================
Archivos procesados : {len(frames)}
Archivos omitidos   : {len(skipped)} {skipped if skipped else ''}
Registros brutos    : {total_raw}
Registros válidos   : {len(df_combined)}
Registros en MySQL  : {records_loaded}
Duración            : {duration:.1f}s
------------------------------------
Completitud  : {metrics['completitud']}%
Exactitud    : {metrics['exactitud']}%
Consistencia : {metrics['consistencia']}%
Score total  : {metrics['quality_score']}%
====================================
"""
    logger.info(report)
    with open(f"data/reports/etl_{datetime.now().strftime('%Y%m%d_%H%M')}.txt", "w") as f:
        f.write(report)

    logger.info("=" * 50)
    logger.info(f"ETL COMPLETADO en {duration:.1f}s")
    logger.info("=" * 50)

    return {
        "files_processed": len(frames),
        "files_skipped":   len(skipped),
        "records_raw":     total_raw,
        "records_processed": len(df_combined),
        "records_loaded":  records_loaded,
        "duration_seconds": round(duration, 2),
        **metrics,
    }


if __name__ == "__main__":
    run_etl()
