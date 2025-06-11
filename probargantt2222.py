import streamlit as st
import pyodbc
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import logging

# Configuración de la página
st.set_page_config(
    page_title="Seguimiento de Partidas",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constantes
CLIENT_IDS = (47, 49, 91, 93, 111, 1445, 2533, 2637, 4294, 4323, 4374, 4411, 4413, 4469, 5506, 6577)
EXCLUDED_ORDERS = (461444, 452744, 459212, 463325, 471285, 471287, 471290, 458803)
MIN_DATE = '2024-07-01'

# Estilos CSS
st.markdown("""
    <style>
    .metric-container {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .highlight-yellow {
        background-color: yellow !important;
    }
    .dataframe-container {
        margin: 1rem 0;
    }
    </style>
    """, unsafe_allow_html=True)

@st.cache_resource
def get_db_connection():
    """Crear conexión a la base de datos con manejo de errores."""
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={st.secrets['server']};"
            f"DATABASE={st.secrets['database']};"
            f"UID={st.secrets['username']};"
            f"PWD={st.secrets['password']};"
            f"TrustServerCertificate=yes;"
        )
        return conn
    except Exception as e:
        st.error(f"Error conectando a la base de datos: {e}")
        logger.error(f"Database connection error: {e}")
        return None

def execute_query(query: str, params: Optional[tuple] = None) -> pd.DataFrame:
    """Ejecutar consulta SQL con manejo de errores."""
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        df = pd.read_sql(query, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Error ejecutando consulta: {e}")
        logger.error(f"Query execution error: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def get_base_query_parts():
    """Obtener partes comunes de las consultas."""
    base_select = """
        a.CoddocOrdenProduccion AS PARTIDA,
        DATEDIFF(DAY, a.dtFechaEmision, GETDATE()) AS DIAS,
        LEFT(f.NommaeItemInventario, 35) AS TELA,
        FORMAT(a.dtFechaEmision, 'dd-MM') AS F_EMISION,
        a.dCantidad AS KG,
        a.nvDocumentoReferencia AS REF,
        g.NommaeColor AS COLOR,
        LEFT(h.NommaeAnexoCliente, 15) AS Cliente,
        a.ntEstado AS ESTADO
    """
    
    base_joins = """
        FROM docOrdenProduccion a WITH (NOLOCK)
        INNER JOIN maeItemInventario f WITH (NOLOCK) ON f.IdmaeItem_Inventario = a.IdmaeItem
        INNER JOIN maeColor g WITH (NOLOCK) ON g.IdmaeColor = a.IdmaeColor
        INNER JOIN maeAnexoCliente h WITH (NOLOCK) ON h.IdmaeAnexo_Cliente = a.IdmaeAnexo_Cliente
        INNER JOIN maeruta k ON a.IdmaeRuta = k.IdmaeRuta
    """
    
    base_conditions = f"""
        WHERE a.IdtdDocumentoForm = 138
        AND a.dtFechaEmision > '{MIN_DATE}'
        AND a.IdmaeAnexo_Cliente IN {CLIENT_IDS}
    """
    
    return base_select, base_joins, base_conditions

@st.cache_data(ttl=300)  # Cache por 5 minutos
def get_partidas_sin_tenido(dias: int) -> pd.DataFrame:
    """Obtener partidas sin teñir."""
    base_select, base_joins, base_conditions = get_base_query_parts()
    
    query = f"""
        SELECT {base_select},
               CASE WHEN LOWER(k.NommaeRuta) LIKE '%mofijado%' THEN 1 ELSE 0 END AS FLAG
        {base_joins}
        LEFT JOIN docRecetaOrdenProduccion i ON a.IdDocumento_OrdenProduccion = i.IdDocumento_OrdenProduccion
        LEFT JOIN docReceta j ON i.IdDocumento_Receta = j.IdDocumento_Receta
        {base_conditions}
        AND (j.dtFechaHoraFin IS NULL OR j.bAnulado = 1)
        AND DATEDIFF(DAY, a.dtFechaEmision, GETDATE()) > ?
    """
    
    df = execute_query(query, (dias,))
    if not df.empty:
        df['KG'] = df['KG'].round(1)
    return df

@st.cache_data(ttl=300)
def get_partidas_con_tenido_sin_aprob(dias: int, include_estamp: bool = False) -> pd.DataFrame:
    """Obtener partidas teñidas pero no aprobadas."""
    base_select, base_joins, base_conditions = get_base_query_parts()
    
    estamp_condition = "LIKE '%estamp%'" if include_estamp else "NOT LIKE '%estamp%'"
    excluded_orders_str = ','.join(map(str, EXCLUDED_ORDERS))
    
    query = f"""
        SELECT {base_select},
               DATEDIFF(DAY, MAX(j.dtFechaHoraFin), GETDATE()) AS DIAS_TEN,
               FORMAT(MAX(j.dtFechaHoraFin), 'dd-MM') AS F_TENIDO
        {base_joins}
        INNER JOIN docRecetaOrdenProduccion i ON a.IdDocumento_OrdenProduccion = i.IdDocumento_OrdenProduccion
        INNER JOIN docReceta j ON i.IdDocumento_Receta = j.IdDocumento_Receta
        {base_conditions}
        AND a.IdDocumento_OrdenProduccion NOT IN ({excluded_orders_str})
        AND j.dtFechaHoraFin IS NOT NULL
        AND j.bAnulado = 0
        AND a.FechaCierreAprobado IS NULL
        AND LOWER(k.NommaeRuta) {estamp_condition}
        GROUP BY a.CoddocOrdenProduccion, a.dtFechaEmision, f.NommaeItemInventario,
                 a.dCantidad, a.nvDocumentoReferencia, g.NommaeColor,
                 h.NommaeAnexoCliente, a.ntEstado
        HAVING DATEDIFF(DAY, MAX(j.dtFechaHoraFin), GETDATE()) > ?
    """
    
    df = execute_query(query, (dias,))
    if not df.empty:
        df['KG'] = df['KG'].round(1)
    return df

def highlight_mofijado(row):
    """Resaltar filas con FLAG = 1."""
    if 'FLAG' in row and row['FLAG'] == 1:
        return ['background-color: yellow' for _ in row]
    return ['' for _ in row]

def display_results(df: pd.DataFrame, title: str, highlight_flag: bool = False):
    """Mostrar resultados con métricas y tabla."""
    if df.empty:
        st.warning(f"No se encontraron datos para: {title}")
        return
    
    # Métricas
    total_registros = len(df)
    total_kg = df['KG'].sum()
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Registros", total_registros)
    with col2:
        st.metric("Total KG", f"{total_kg:.1f}")
    
    # Tabla
    if highlight_flag and 'FLAG' in df.columns:
        styled_df = df.style.apply(highlight_mofijado, axis=1).format({"KG": "{:.1f}"})
        st.dataframe(styled_df, use_container_width=True)
    else:
        st.dataframe(df.style.format({"KG": "{:.1f}"}), use_container_width=True)

def main():
    """Función principal de la aplicación."""
    st.title("📊 Seguimiento de Partidas")
    st.markdown("---")
    
    # Sidebar para configuración
    with st.sidebar:
        st.header("⚙️ Configuración")
        
        # Configuración para partidas sin teñir
        st.subheader("Partidas sin TEÑIR")
        dias_sin_tenido = st.number_input(
            "Días sin teñir",
            min_value=1,
            value=8,
            help="Días desde la emisión sin proceso de teñido"
        )
        
        # Configuración para partidas teñidas
        st.subheader("Partidas TEÑIDAS")
        dias_con_tenido = st.number_input(
            "Días desde teñido (sin estampado)",
            min_value=1,
            value=5,
            help="Días desde el teñido para partidas sin estampado"
        )
        
        dias_con_tenido_estamp = st.number_input(
            "Días desde teñido (con estampado)",
            min_value=1,
            value=20,
            help="Días desde el teñido para partidas con estampado"
        )
        
        st.markdown("---")
        auto_refresh = st.checkbox("Auto-actualizar", value=False)
        if auto_refresh:
            st.rerun()
    
    # Contenido principal
    tab1, tab2, tab3 = st.tabs([
        "🔴 Sin Teñir",
        "🟡 Teñidas (Sin Estampado)",
        "🟠 Teñidas (Con Estampado)"
    ])
    
    with tab1:
        st.header("Partidas sin TEÑIR")
        if st.button("🔍 Consultar Partidas sin Teñir", key="btn_sin_tenido"):
            with st.spinner("Consultando datos..."):
                df_sin_tenido = get_partidas_sin_tenido(dias_sin_tenido)
                display_results(df_sin_tenido, "Partidas sin teñir", highlight_flag=True)
    
    with tab2:
        st.header("Partidas TEÑIDAS pero no APROBADAS (Sin Estampado)")
        if st.button("🔍 Consultar Partidas Teñidas", key="btn_con_tenido"):
            with st.spinner("Consultando datos..."):
                df_con_tenido = get_partidas_con_tenido_sin_aprob(dias_con_tenido, include_estamp=False)
                display_results(df_con_tenido, "Partidas teñidas sin aprobar")
    
    with tab3:
        st.header("Partidas TEÑIDAS pero no APROBADAS (Con Estampado)")
        if st.button("🔍 Consultar Partidas con Estampado", key="btn_con_estamp"):
            with st.spinner("Consultando datos..."):
                df_con_estamp = get_partidas_con_tenido_sin_aprob(dias_con_tenido_estamp, include_estamp=True)
                display_results(df_con_estamp, "Partidas teñidas con estampado")
    
    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #666;'>"
        "💡 Tip: Utiliza el panel lateral para ajustar los parámetros de búsqueda"
        "</div>",
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
