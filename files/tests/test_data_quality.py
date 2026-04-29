"""
Pruebas automatizadas de calidad de datos.
Ejecutar con: pytest tests/ -v
"""
import pytest
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


# ─────────────── Fixtures ───────────────
@pytest.fixture(scope="module")
def raw_files():
    """Lee todos los CSV disponibles en data/."""
    from etl import extract_all
    files = extract_all("data")
    if not files:
        pytest.skip("No hay archivos CSV en data/ para pruebas")
    return files


@pytest.fixture(scope="module")
def df_clean(raw_files):
    """Aplica schema mapping + transformación a todos los archivos."""
    from etl import detect_and_map_schema, transform
    import pandas as pd
    frames = []
    for df_raw, filename in raw_files:
        df_mapped = detect_and_map_schema(df_raw, filename)
        if df_mapped is not None:
            frames.append(transform(df_mapped, filename))
    if not frames:
        pytest.skip("Ningún archivo pudo mapearse al esquema estándar")
    return pd.concat(frames, ignore_index=True)


@pytest.fixture(scope="module")
def df_single(raw_files):
    """Primer archivo disponible para pruebas individuales."""
    from etl import detect_and_map_schema, transform
    df_raw, filename = raw_files[0]
    df_mapped = detect_and_map_schema(df_raw, filename)
    if df_mapped is None:
        pytest.skip(f"No se pudo mapear {filename}")
    return transform(df_mapped, filename)


# ─────────────── Pruebas de schema mapping ───────────────
class TestSchemaMapping:
    def test_detecta_al_menos_un_archivo(self, raw_files):
        assert len(raw_files) > 0, "No se encontró ningún CSV en data/"

    def test_mapeo_exitoso(self, raw_files):
        from etl import detect_and_map_schema
        exitosos = 0
        for df_raw, filename in raw_files:
            if detect_and_map_schema(df_raw, filename) is not None:
                exitosos += 1
        assert exitosos > 0, "Ningún archivo pudo mapearse — revisa SCHEMA en etl.py"

    def test_columna_source_file_existe(self, df_clean):
        assert "source_file" in df_clean.columns, "Falta columna source_file"

    def test_source_file_no_vacio(self, df_clean):
        vacios = df_clean["source_file"].isnull().sum()
        assert vacios == 0, f"{vacios} registros sin source_file"

    def test_multiples_fuentes_si_hay_varios_csv(self, raw_files, df_clean):
        if len(raw_files) > 1:
            fuentes = df_clean["source_file"].nunique()
            assert fuentes > 1, "Con varios CSVs debe haber múltiples source_file"


# ─────────────── Pruebas de esquema ───────────────
class TestEsquema:
    COLUMNAS_REQUERIDAS = [
        "Order ID", "Date", "Amount", "Qty",
        "Category", "ship-state", "Courier Status",
        "Year", "Month", "Price_per_unit", "source_file",
    ]

    def test_columnas_existen(self, df_clean):
        for col in self.COLUMNAS_REQUERIDAS:
            assert col in df_clean.columns, f"Columna faltante: {col}"

    def test_tipos_numericos(self, df_clean):
        assert pd.api.types.is_numeric_dtype(df_clean["Amount"]), "Amount debe ser numérico"
        assert pd.api.types.is_numeric_dtype(df_clean["Qty"]),    "Qty debe ser numérico"

    def test_tipo_fecha(self, df_clean):
        assert pd.api.types.is_datetime64_any_dtype(df_clean["Date"]), "Date debe ser datetime"


# ─────────────── Pruebas de integridad ───────────────
class TestIntegridad:
    def test_sin_order_id_nulo(self, df_clean):
        nulos = df_clean["Order ID"].isnull().sum()
        assert nulos == 0, f"Hay {nulos} Order ID nulos"

    def test_sin_fecha_nula(self, df_clean):
        nulos = df_clean["Date"].isnull().sum()
        assert nulos == 0, f"Hay {nulos} fechas nulas"

    def test_sin_amount_nulo(self, df_clean):
        nulos = df_clean["Amount"].isnull().sum()
        assert nulos == 0, f"Hay {nulos} montos nulos"

    def test_dataset_no_vacio(self, df_clean):
        assert len(df_clean) > 0, "El dataset combinado está vacío"


# ─────────────── Pruebas de reglas de negocio ───────────────
class TestReglasNegocio:
    def test_sin_ventas_negativas(self, df_clean):
        neg = (df_clean["Amount"] <= 0).sum()
        assert neg == 0, f"Hay {neg} registros con Amount <= 0"

    def test_sin_cantidad_negativa(self, df_clean):
        neg = (df_clean["Qty"] <= 0).sum()
        assert neg == 0, f"Hay {neg} registros con Qty <= 0"

    def test_precio_por_unidad_positivo(self, df_clean):
        inv = (df_clean["Price_per_unit"] <= 0).sum()
        assert inv == 0, f"Hay {inv} Price_per_unit inválidos"

    def test_year_valido(self, df_clean):
        for y in df_clean["Year"].dropna().unique():
            assert 2000 <= y <= 2100, f"Año inválido: {y}"

    def test_month_valido(self, df_clean):
        for m in df_clean["Month"].dropna().unique():
            assert 1 <= m <= 12, f"Mes inválido: {m}"


# ─────────────── Pruebas de KPIs ───────────────
class TestKPIsCalidad:
    def test_completitud_mayor_95(self, df_clean):
        v = df_clean.notnull().mean().mean() * 100
        assert v >= 95, f"Completitud {v:.2f}% < 95%"

    def test_exactitud_mayor_98(self, df_clean):
        v = ((df_clean["Amount"] > 0) & (df_clean["Qty"] > 0)).mean() * 100
        assert v >= 98, f"Exactitud {v:.2f}% < 98%"

    def test_consistencia_mayor_97(self, df_clean):
        v = (df_clean["Price_per_unit"] > 0).mean() * 100
        assert v >= 97, f"Consistencia {v:.2f}% < 97%"

    def test_score_general(self, df_clean):
        c = df_clean.notnull().mean().mean() * 100
        e = ((df_clean["Amount"] > 0) & (df_clean["Qty"] > 0)).mean() * 100
        co = (df_clean["Price_per_unit"] > 0).mean() * 100
        score = (c + e + co) / 3
        assert score >= 90, f"Score general {score:.2f}% demasiado bajo"


# ─────────────── Pruebas de integración ───────────────
class TestETLIntegracion:
    def test_etl_reduce_registros(self, raw_files, df_clean):
        total_raw = sum(len(df) for df, _ in raw_files)
        assert len(df_clean) <= total_raw, "El ETL no debe generar más registros que el original"

    def test_etl_elimina_invalidos(self, df_clean):
        inv = ((df_clean["Amount"] <= 0) | (df_clean["Qty"] <= 0)).sum()
        assert inv == 0, "El ETL debe eliminar todos los registros inválidos"

    def test_csv_limpio_generado(self, df_clean):
        df_clean.to_csv("data/clean_sales.csv", index=False)
        assert os.path.exists("data/clean_sales.csv"), "No se generó el CSV limpio"
        df_out = pd.read_csv("data/clean_sales.csv")
        assert len(df_out) > 0, "El CSV limpio está vacío"

    def test_run_etl_completo(self):
        """Prueba el pipeline completo de punta a punta."""
        from etl import run_etl
        try:
            result = run_etl()
            assert result["files_processed"] > 0, "No se procesó ningún archivo"
            assert result["records_processed"] > 0, "No hay registros procesados"
            assert result["quality_score"] >= 90, "Score de calidad muy bajo"
        except FileNotFoundError:
            pytest.skip("No hay archivos CSV disponibles para prueba de integración")
