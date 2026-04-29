import streamlit as st
import pandas as pd
import plotly.express as px
import os
import hashlib
import json

st.set_page_config(page_title="Dashboard Ventas", layout="wide")

# =========================
#  RBAC — CARGA DE CREDENCIALES DESDE SECRETS
# =========================
def load_users():
    """
    Carga usuarios desde variable de entorno APP_USERS (JSON).
    Si no existe, usa usuarios por defecto de desarrollo.
    """
    users_env = os.getenv("APP_USERS")
    if users_env:
        return json.loads(users_env)
    # Fallback desarrollo — NO usar en producción
    return {
        "admin": {
            "name":     "Administrador",
            "password": "$2b$12$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi",
            "role":     "admin",
        },
        "viewer1": {
            "name":     "Viewer Uno",
            "password": "$2b$12$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi",
            "role":     "viewer",
        },
        "viewer2": {
            "name":     "Viewer Dos",
            "password": "$2b$12$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi",
            "role":     "viewer",
        },
    }

USERS = load_users()

PERMISOS = {
    "admin": {
        "ver_kpis":           True,
        "ver_graficas":       True,
        "ver_calidad":        True,
        "ver_datos_raw":      True,
        "descargar_datos":    True,
        "ver_todos_datasets": True,
        "ver_insights":       True,
    },
    "viewer": {
        "ver_kpis":           True,
        "ver_graficas":       True,
        "ver_calidad":        False,
        "ver_datos_raw":      False,
        "descargar_datos":    False,
        "ver_todos_datasets": False,
        "ver_insights":       True,
    },
}


def check_password(username: str, password: str) -> bool:
    user = USERS.get(username)
    if not user:
        return False
    secret = os.getenv("APP_SECRET", "sales_dashboard_2024_secret")
    hashed = hashlib.sha256((secret + password).encode()).hexdigest()
    return hashed == user["password"]


# =========================
#  LOGIN
# =========================
def login():
    st.markdown("""
        <style>
        .block-container { max-width: 420px; margin: 60px auto; }
        </style>
    """, unsafe_allow_html=True)

    st.title("Sistema de Ventas")
    st.subheader("Iniciar sesión")

    username = st.text_input("Usuario")
    password = st.text_input("Contraseña", type="password")

    if st.button("Entrar", use_container_width=True, type="primary"):
        if check_password(username, password):
            st.session_state["authenticated"] = True
            st.session_state["username"]      = username
            st.session_state["name"]          = USERS[username]["name"]
            st.session_state["role"]          = USERS[username]["role"]
            st.rerun()
        else:
            st.error("Usuario o contraseña incorrectos")

    st.markdown("---")
    st.caption("admin / Admin123! — acceso completo")
    st.caption("viewer1 / Viewer123! — solo lectura")


def logout():
    for key in ["authenticated", "username", "name", "role"]:
        st.session_state.pop(key, None)
    st.rerun()


# =========================
#  VERIFICAR SESIÓN
# =========================
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if not st.session_state["authenticated"]:
    login()
    st.stop()

username = st.session_state["username"]
name     = st.session_state["name"]
role     = st.session_state["role"]
perms    = PERMISOS[role]

# =========================
#  HEADER
# =========================
col_title, col_user = st.columns([4, 1])
with col_title:
    st.title("Sistema de Análisis de Ventas (DataOps + DevOps)")
with col_user:
    st.markdown(f"**{name}**")
    badge_color = "#1D9E75" if role == "admin" else "#378ADD"
    st.markdown(
        f"<span style='background:{badge_color};color:white;padding:2px 10px;"
        f"border-radius:20px;font-size:12px'>{role.upper()}</span>",
        unsafe_allow_html=True,
    )
    if st.button("Cerrar sesión"):
        logout()


# =========================
#  CARGA DE DATOS
# =========================
@st.cache_data(ttl=300)
def load_data():
    try:
        from sqlalchemy import create_engine
        host     = os.getenv("MYSQL_HOST", "localhost")
        user     = os.getenv("MYSQL_USER", "sales_user")
        password = os.getenv("MYSQL_PASSWORD", "sales_password")
        database = os.getenv("MYSQL_DATABASE", "sales_db")
        engine   = create_engine(
            f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"
        )
        df = pd.read_sql("SELECT * FROM sales", engine)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.rename(columns={
            "order_id":       "Order ID",
            "date":           "Date",
            "amount":         "Amount",
            "qty":            "Qty",
            "category":       "Category",
            "ship_state":     "ship-state",
            "courier_status": "Courier Status",
            "year":           "Year",
            "month":          "Month",
            "price_per_unit": "Price_per_unit",
        })
        st.sidebar.success("Fuente: MySQL")
        return df
    except Exception:
        pass

    if os.path.exists("data/clean_sales.csv"):
        df = pd.read_csv("data/clean_sales.csv")
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df["Qty"]    = pd.to_numeric(df.get("Qty",    df.get("qty",    0)), errors="coerce")
        df["Amount"] = pd.to_numeric(df.get("Amount", df.get("amount", 0)), errors="coerce")
        if "Year"           not in df.columns: df["Year"]           = df["Date"].dt.year
        if "Month"          not in df.columns: df["Month"]          = df["Date"].dt.month
        if "Price_per_unit" not in df.columns: df["Price_per_unit"] = df["Amount"] / df["Qty"].replace(0, 1)
        if "Category"       not in df.columns: df["Category"]       = "Unknown"
        if "ship-state"     not in df.columns: df["ship-state"]     = df.get("ship_state", "Unknown")
        if "Courier Status" not in df.columns: df["Courier Status"] = df.get("courier_status", "Unknown")
        if "Order ID"       not in df.columns: df["Order ID"]       = df.get("order_id", df.index.astype(str))
        if "source_file"    not in df.columns: df["source_file"]    = "datos.csv"
        df["Category"]       = df["Category"].fillna("Unknown")
        df["ship-state"]     = df["ship-state"].fillna("Unknown")
        df["Courier Status"] = df["Courier Status"].fillna("Unknown")
        st.sidebar.info("Fuente: CSV limpio")
        return df

    st.error("No hay datos disponibles.")
    st.stop()


df_all = load_data()

# =========================
#  SIDEBAR — SELECTOR DE DATASET
# =========================
st.sidebar.header("Dataset")
datasets_disponibles = sorted(df_all["source_file"].dropna().unique()) if "source_file" in df_all.columns else []

if perms["ver_todos_datasets"]:
    opciones = ["Todos los datasets"] + datasets_disponibles
else:
    opciones = datasets_disponibles if datasets_disponibles else ["Sin datos"]

dataset_sel = st.sidebar.selectbox("Seleccionar dataset", opciones)

if dataset_sel == "Todos los datasets":
    df_base = df_all.copy()
    st.sidebar.caption(f"{len(df_all):,} registros · {len(datasets_disponibles)} dataset(s)")
else:
    df_base = df_all[df_all["source_file"] == dataset_sel].copy() if "source_file" in df_all.columns else df_all.copy()
    st.sidebar.caption(f"{len(df_base):,} registros")

# =========================
#  SIDEBAR — FILTROS
# =========================
st.sidebar.header("Filtros")
years    = sorted(df_base["Year"].dropna().unique())
year     = st.sidebar.selectbox("Año", years) if years else None
category = st.sidebar.multiselect("Categoría", sorted(df_base["Category"].dropna().unique()))
state    = st.sidebar.multiselect("Estado",     sorted(df_base["ship-state"].dropna().unique()))

filtered_df = df_base[df_base["Year"] == year] if year else df_base
if category: filtered_df = filtered_df[filtered_df["Category"].isin(category)]
if state:    filtered_df = filtered_df[filtered_df["ship-state"].isin(state)]

if dataset_sel == "Todos los datasets":
    st.info(f"Mostrando: {', '.join(datasets_disponibles)}")
else:
    st.info(f"Dataset activo: {dataset_sel}")

# =========================
#  KPIs
# =========================
if perms["ver_kpis"]:
    total_sales    = filtered_df["Amount"].sum()
    total_orders   = filtered_df["Order ID"].nunique()
    avg_ticket     = total_sales / total_orders if total_orders > 0 else 0
    total_products = filtered_df["Qty"].sum()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Ventas Totales",     f"${total_sales:,.2f}")
    col2.metric("Órdenes",            total_orders)
    col3.metric("Productos Vendidos", int(total_products))
    col4.metric("Ticket Promedio",    f"${avg_ticket:,.2f}")

# =========================
#  GRÁFICAS
# =========================
if perms["ver_graficas"]:
    sales_by_month    = filtered_df.groupby("Month")["Amount"].sum().reset_index()
    sales_by_category = filtered_df.groupby("Category")["Amount"].sum().reset_index()

    st.subheader("Ventas por Mes")
    st.plotly_chart(px.bar(sales_by_month, x="Month", y="Amount"), use_container_width=True)

    st.subheader("Ventas por Categoría")
    st.plotly_chart(px.pie(sales_by_category, names="Category", values="Amount"), use_container_width=True)

    st.subheader("Top Estados")
    top_states = filtered_df.groupby("ship-state")["Amount"].sum().sort_values(ascending=False).head(10).reset_index()
    st.plotly_chart(px.bar(top_states, x="ship-state", y="Amount"), use_container_width=True)

    st.subheader("Estado de Envíos")
    status_counts = filtered_df["Courier Status"].value_counts().reset_index()
    status_counts.columns = ["Estado", "Cantidad"]
    st.plotly_chart(px.bar(status_counts, x="Estado", y="Cantidad"), use_container_width=True)

    if dataset_sel == "Todos los datasets" and len(datasets_disponibles) > 1:
        st.subheader("Comparativa entre datasets")
        comp = filtered_df.groupby("source_file")["Amount"].sum().reset_index()
        comp.columns = ["Dataset", "Ventas Totales"]
        st.plotly_chart(px.bar(comp, x="Dataset", y="Ventas Totales", color="Dataset"), use_container_width=True)

# =========================
#  INSIGHTS
# =========================
if perms["ver_insights"] and len(filtered_df) > 0:
    st.subheader("Insights Automáticos")
    best_cat   = sales_by_category.sort_values("Amount", ascending=False).iloc[0]["Category"]
    best_month = sales_by_month.sort_values("Amount",    ascending=False).iloc[0]["Month"]
    st.success(f"La categoría con más ventas es: {best_cat}")
    st.info(f"El mes con mayores ventas fue: {best_month}")

# =========================
#  CALIDAD (solo admin)
# =========================
if perms["ver_calidad"]:
    st.subheader("Calidad de Datos")
    cols_criticas  = ["Order ID", "Date", "Amount", "Qty", "Category", "ship-state", "Courier Status"]
    cols_presentes = [c for c in cols_criticas if c in filtered_df.columns]
    completitud    = filtered_df[cols_presentes].notnull().mean().mean() * 100
    exactitud      = ((filtered_df["Amount"] > 0) & (filtered_df["Qty"] > 0)).mean() * 100
    consistencia   = (filtered_df["Price_per_unit"] > 0).mean() * 100
    quality_score  = (completitud + exactitud + consistencia) / 3

    c1, c2, c3 = st.columns(3)
    c1.metric("Completitud",  f"{completitud:.2f}%")
    c2.metric("Exactitud",    f"{exactitud:.2f}%")
    c3.metric("Consistencia", f"{consistencia:.2f}%")
    st.metric("Score General", f"{quality_score:.2f}%")

    if completitud  < 95: st.error("Baja completitud")
    if exactitud    < 98: st.warning("Baja exactitud")
    if consistencia < 97: st.warning("Baja consistencia")
else:
    st.warning("Los datos de calidad son visibles solo para administradores.")

# =========================
#  DATOS RAW (solo admin)
# =========================
if perms["ver_datos_raw"]:
    st.subheader("Datos Procesados")
    st.dataframe(filtered_df.head(100))

# =========================
#  DESCARGA (solo admin)
# =========================
if perms["descargar_datos"]:
    st.subheader("Descargar datos")
    csv = filtered_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Descargar datos filtrados",
        data=csv,
        file_name=f"datos_{dataset_sel.replace(' ', '_')}.csv",
        mime="text/csv",
    )
else:
    st.warning("La descarga solo está disponible para administradores.")
