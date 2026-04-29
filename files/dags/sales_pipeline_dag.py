from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging
import sys
import os

sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

# ─────────────── Configuración del DAG ───────────────
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="sales_pipeline",
    description="Pipeline ETL de ventas con validación de calidad de datos",
    default_args=default_args,
    schedule_interval="0 6 * * *",   # cada día a las 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "sales", "data-quality"],
)


# ─────────────── Tarea 1: Extracción ───────────────
def task_extract(**context):
    from etl import extract_all
    raw_files = extract_all("/opt/airflow/data")
    total = sum(len(df) for df, _ in raw_files)
    context["ti"].xcom_push(key="raw_count", value=total)
    logger.info(f"Extracción: {total} registros crudos de {len(raw_files)} archivo(s)")


# ─────────────── Tarea 2: Transformación ───────────────
def task_transform(**context):
    import pandas as pd
    from etl import extract_all, detect_and_map_schema, transform
    raw_files = extract_all("/opt/airflow/data")
    frames = []
    for df_raw, filename in raw_files:
        df_mapped = detect_and_map_schema(df_raw, filename)
        if df_mapped is not None:
            frames.append(transform(df_mapped, filename))
    if not frames:
        raise ValueError("Ningún archivo pudo procesarse — revisa SCHEMA en etl.py")
    df_clean = pd.concat(frames, ignore_index=True)
    os.makedirs("/opt/airflow/data", exist_ok=True)
    df_clean.to_csv("/opt/airflow/data/clean_sales.csv", index=False)
    context["ti"].xcom_push(key="clean_count", value=len(df_clean))
    logger.info(f"Transformación: {len(df_clean)} registros limpios")


# ─────────────── Tarea 3: Validación de calidad ───────────────
def task_validate(**context):
    import pandas as pd
    from etl import validate_quality
    df = pd.read_csv("/opt/airflow/data/clean_sales.csv")
    metrics = validate_quality(df)
    for k, v in metrics.items():
        context["ti"].xcom_push(key=k, value=v)
    logger.info(f"Métricas de calidad: {metrics}")


# ─────────────── Tarea 4: Decisión por calidad ───────────────
def task_branch_quality(**context):
    ti = context["ti"]
    completitud  = ti.xcom_pull(key="completitud")
    exactitud    = ti.xcom_pull(key="exactitud")
    consistencia = ti.xcom_pull(key="consistencia")

    if completitud >= 95 and exactitud >= 98 and consistencia >= 97:
        logger.info("Calidad OK — procediendo a carga")
        return "cargar_mysql"
    else:
        logger.warning("Calidad INSUFICIENTE — enviando alerta")
        return "alerta_calidad"


# ─────────────── Tarea 5: Carga a MySQL ───────────────
def task_load(**context):
    import pandas as pd
    from etl import get_mysql_engine, load
    df = pd.read_csv("/opt/airflow/data/clean_sales.csv")
    engine = get_mysql_engine()
    n = load(df, engine)
    context["ti"].xcom_push(key="records_loaded", value=n)
    logger.info(f"Carga completada: {n} registros en MySQL")


# ─────────────── Tarea 6: Reporte de calidad ───────────────
def task_quality_report(**context):
    ti = context["ti"]
    completitud  = ti.xcom_pull(key="completitud")
    exactitud    = ti.xcom_pull(key="exactitud")
    consistencia = ti.xcom_pull(key="consistencia")
    score        = ti.xcom_pull(key="quality_score")
    loaded       = ti.xcom_pull(key="records_loaded") or 0

    report = f"""
====================================
REPORTE DE CALIDAD - {datetime.now().strftime('%Y-%m-%d %H:%M')}
====================================
Completitud  : {completitud}%  (meta >95%)
Exactitud    : {exactitud}%   (meta >98%)
Consistencia : {consistencia}%  (meta >97%)
Score total  : {score}%
Registros cargados: {loaded}
====================================
"""
    logger.info(report)
    os.makedirs("/opt/airflow/data/reports", exist_ok=True)
    with open(f"/opt/airflow/data/reports/quality_{datetime.now().strftime('%Y%m%d')}.txt", "w") as f:
        f.write(report)


# ─────────────── Definición de tareas ───────────────
t_extract = PythonOperator(
    task_id="extraer_datos",
    python_callable=task_extract,
    dag=dag,
)

t_transform = PythonOperator(
    task_id="transformar_datos",
    python_callable=task_transform,
    dag=dag,
)

t_validate = PythonOperator(
    task_id="validar_calidad",
    python_callable=task_validate,
    dag=dag,
)

t_branch = BranchPythonOperator(
    task_id="decision_calidad",
    python_callable=task_branch_quality,
    dag=dag,
)

t_load = PythonOperator(
    task_id="cargar_mysql",
    python_callable=task_load,
    dag=dag,
)

t_alert = BashOperator(
    task_id="alerta_calidad",
    bash_command='echo "ALERTA: Calidad de datos insuficiente - revisar logs" && exit 1',
    dag=dag,
)

t_report = PythonOperator(
    task_id="generar_reporte",
    python_callable=task_quality_report,
    trigger_rule="none_failed_min_one_success",
    dag=dag,
)

t_end = EmptyOperator(task_id="fin", trigger_rule="none_failed_min_one_success", dag=dag)

# ─────────────── Dependencias ───────────────
t_extract >> t_transform >> t_validate >> t_branch
t_branch >> t_load >> t_report >> t_end
t_branch >> t_alert >> t_end
