import streamlit as st
import pyodbc
import pandas as pd
from datetime import datetime, timedelta

st.set_page_config(layout="wide")

# Función para conectar a SQL Server
def connect_to_db():
    conn = pyodbc.connect(
        "driver={odbc driver 17 for sql server};"
        "server=" + st.secrets["server"] + ";"
        "database=" + st.secrets["database"] + ";"
        "uid=" + st.secrets["username"] + ";"
        "pwd=" + st.secrets["password"] + ";"
    )
    return conn


# Consulta para obtener PARTIDAS sin F_TENIDO y con más de x días
def get_partidas_sin_tenido(dias):
    conn = connect_to_db()
    query = f"""
        SELECT a.CoddocOrdenProduccion AS PARTIDA, DATEDIFF(DAY, a.dtFechaEmision, GETDATE()) AS DIAS  , LEFT(f.NommaeItemInventario, 35) AS TELA, FORMAT(a.dtFechaEmision, 'dd-MM') AS F_EMISION, 
               --FORMAT(j.dtFechaHoraFin, 'dd-MM') AS F_TENIDO, 
               --FORMAT(a.FechaCierreAprobado, 'dd-MM') AS F_APROB_TELA, 
               a.dCantidad AS KG, 
               a.nvDocumentoReferencia AS REF, g.NommaeColor AS COLOR, --a.bCierreAprobado AS AP_DES, 
               --a.bProduccionAprobado AS DESP, a.bcerrado AS CERR, 
               LEFT(h.NommaeAnexoCliente, 15) AS Cliente, 
               --a.ntEstado AS ESTADO, k.NommaeRuta AS RUTA,
               CASE WHEN LOWER(k.NommaeRuta) LIKE '%mofijado%' THEN 1 ELSE 0 END AS FLAG
        FROM docOrdenProduccion a WITH (NOLOCK)
        INNER JOIN maeItemInventario f WITH (NOLOCK) ON f.IdmaeItem_Inventario = a.IdmaeItem
        INNER JOIN maeColor g WITH (NOLOCK) ON g.IdmaeColor = a.IdmaeColor
        INNER JOIN maeAnexoCliente h WITH (NOLOCK) ON h.IdmaeAnexo_Cliente = a.IdmaeAnexo_Cliente
        --INNER JOIN docRecetaOrdenProduccion i ON a.IdDocumento_OrdenProduccion = i.IdDocumento_OrdenProduccion
        --INNER JOIN docReceta j ON i.IdDocumento_Receta = j.IdDocumento_Receta
        LEFT JOIN docRecetaOrdenProduccion i ON a.IdDocumento_OrdenProduccion = i.IdDocumento_OrdenProduccion  -- LEFT JOIN
        LEFT JOIN docReceta j ON i.IdDocumento_Receta = j.IdDocumento_Receta
        INNER JOIN maeruta k ON a.IdmaeRuta = k.IdmaeRuta
        WHERE a.IdtdDocumentoForm = 138
        AND j.dtFechaHoraFin IS NULL
        AND DATEDIFF(DAY, a.dtFechaEmision, GETDATE()) > {dias}
        AND a.dtFechaEmision > '01-01-2025'
        and j.bAnulado =0
        AND a.IdmaeAnexo_Cliente IN (47, 49, 91, 93, 111, 1445, 2533, 2637, 4294, 4323, 4374, 4411, 4413, 4469, 5506, 6577)
    """
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df

# Consulta para obtener PARTIDAS con F_TENIDO pero sin F_APROB_TELA y que RUTA no contenga "ESTAMP"
def get_partidas_con_tenido_sin_aprob_tela(dias):
    conn = connect_to_db()
    query = f"""
        SELECT a.CoddocOrdenProduccion AS PARTIDA, 
       DATEDIFF(DAY, a.dtFechaEmision, GETDATE()) AS DIAS,  
       DATEDIFF(DAY, MAX(j.dtFechaHoraFin), GETDATE()) AS DIAS_TEN,  -- Última fecha de teñido
       LEFT(f.NommaeItemInventario, 35) AS TELA, 
       FORMAT(a.dtFechaEmision, 'dd-MM') AS F_EMISION, 
       FORMAT(MAX(j.dtFechaHoraFin), 'dd-MM') AS F_TENIDO,  -- Última fecha de teñido
       a.dCantidad AS KG, 
       a.nvDocumentoReferencia AS REF, 
       g.NommaeColor AS COLOR, 
       LEFT(h.NommaeAnexoCliente, 15) AS Cliente,
       a.ntEstado AS ESTADO
FROM docOrdenProduccion a WITH (NOLOCK)
INNER JOIN maeItemInventario f WITH (NOLOCK) ON f.IdmaeItem_Inventario = a.IdmaeItem
INNER JOIN maeColor g WITH (NOLOCK) ON g.IdmaeColor = a.IdmaeColor
INNER JOIN maeAnexoCliente h WITH (NOLOCK) ON h.IdmaeAnexo_Cliente = a.IdmaeAnexo_Cliente
INNER JOIN docRecetaOrdenProduccion i ON a.IdDocumento_OrdenProduccion = i.IdDocumento_OrdenProduccion
INNER JOIN docReceta j ON i.IdDocumento_Receta = j.IdDocumento_Receta
INNER JOIN maeruta k ON a.IdmaeRuta = k.IdmaeRuta
WHERE a.IdtdDocumentoForm = 138
AND NOT a.IdDocumento_OrdenProduccion IN (461444, 452744, 459212, 463325, 471285, 471287, 471290)
AND j.dtFechaHoraFin IS NOT NULL
AND j.bAnulado = 0
AND a.FechaCierreAprobado IS NULL
AND LOWER(k.NommaeRuta) NOT LIKE '%estamp%'
AND a.dtFechaEmision > '01-01-2025'
AND a.IdmaeAnexo_Cliente IN (47, 49, 91, 93, 111, 1445, 2533, 2637, 4294, 4323, 4374, 4411, 4413, 4469, 5506, 6577)
GROUP BY a.CoddocOrdenProduccion, 
         a.dtFechaEmision, 
         f.NommaeItemInventario, 
         a.dCantidad, 
         a.nvDocumentoReferencia, 
         g.NommaeColor, 
         h.NommaeAnexoCliente, 
         a.ntEstado
HAVING DATEDIFF(DAY, MAX(j.dtFechaHoraFin), GETDATE()) > {dias};

        
    """
    df = pd.read_sql(query, conn)
    conn.close()
    # Redondear la columna KG a 1 decimal
    df['KG'] = df['KG'].round(1)
    return df

# Consulta para obtener PARTIDAS con F_TENIDO pero sin F_APROB_TELA y que RUTA contenga "ESTAMP"
def get_partidas_con_tenido_sin_aprob_tela_estamp(dias):
    conn = connect_to_db()
    query = f"""
        SELECT a.CoddocOrdenProduccion AS PARTIDA,DATEDIFF(DAY, a.dtFechaEmision, GETDATE()) AS DIAS    , DATEDIFF(DAY, j.dtFechaHoraFin, GETDATE()) AS DIAS_TEN  , LEFT(f.NommaeItemInventario, 35) AS TELA, FORMAT(a.dtFechaEmision, 'dd-MM') AS F_EMISION,
              FORMAT(j.dtFechaHoraFin, 'dd-MM') AS F_TENIDO, --FORMAT(a.FechaCierreAprobado, 'dd-MM') AS F_APROB_TELA, 
              a.dCantidad AS KG, 
               a.nvDocumentoReferencia AS REF, g.NommaeColor AS COLOR, --a.bCierreAprobado AS AP_DES, 
               --a.bProduccionAprobado AS DESP, --a.bcerrado AS CERR, 
               LEFT(h.NommaeAnexoCliente, 15) AS Cliente,
               a.ntEstado AS ESTADO --, k.NommaeRuta AS RUTA
        FROM docOrdenProduccion a WITH (NOLOCK)
        INNER JOIN maeItemInventario f WITH (NOLOCK) ON f.IdmaeItem_Inventario = a.IdmaeItem
        INNER JOIN maeColor g WITH (NOLOCK) ON g.IdmaeColor = a.IdmaeColor
        INNER JOIN maeAnexoCliente h WITH (NOLOCK) ON h.IdmaeAnexo_Cliente = a.IdmaeAnexo_Cliente
        INNER JOIN docRecetaOrdenProduccion i ON a.IdDocumento_OrdenProduccion = i.IdDocumento_OrdenProduccion
        INNER JOIN docReceta j ON i.IdDocumento_Receta = j.IdDocumento_Receta
        INNER JOIN maeruta k ON a.IdmaeRuta = k.IdmaeRuta
        WHERE a.IdtdDocumentoForm = 138
        AND NOT a.IdDocumento_OrdenProduccion IN (461444, 452744, 459212, 463325, 458803, 471285, 471287)
        AND j.dtFechaHoraFin IS NOT NULL
        and j.bAnulado =0
        AND a.FechaCierreAprobado IS NULL
        AND LOWER(k.NommaeRuta) LIKE '%estamp%'
        AND DATEDIFF(DAY, j.dtFechaHoraFin, GETDATE()) > {dias}
        AND a.dtFechaEmision > '01-01-2025'
        AND a.IdmaeAnexo_Cliente IN (47, 49, 91, 93, 111, 1445, 2533, 2637, 4294, 4323, 4374, 4411, 4413, 4469, 5506, 6577)
    """
    df = pd.read_sql(query, conn)
    conn.close()
    # Redondear la columna KG a 1 decimal
    df['KG'] = df['KG'].round(1)
    return df

def highlight_mofijado(row):
    return ['background-color: yellow' if row['FLAG'] == 1 else '' for _ in row]

# Interfaz de Streamlit
st.title("Seguimiento de Partidas")

# Aplicar estilos personalizados con CSS
st.markdown("""
    <style>
    .input-number-box {
        width: 100px !important;  /* Aproximadamente 5 cm */
    }
    </style>
    """, unsafe_allow_html=True)

# Selección de días para la primera consulta
dias_sin_tenido = st.number_input("Días sin TEÑIR (por defecto 8)", min_value=1, value=8)

# Asignar la clase CSS personalizada al input usando Javascript
#st.markdown("""
   # <script>
    #var number_input = window.parent.document.querySelector('input[data-testid="stNumberInput-number_input_dias"]');
    #if (number_input) {
     #   number_input.parentElement.classList.add('input-number-box');
    #}
    #</script>
    #""", unsafe_allow_html=True)

if st.button("Mostrar partidas no TEÑIDAS"):
    df_sin_tenido = get_partidas_sin_tenido(dias_sin_tenido)

    # Contar los registros y sumar los KG
    total_registros = len(df_sin_tenido)
    total_kg = df_sin_tenido['KG'].sum()
    
    st.write(f"TOTAL REGISTROS  :   {total_registros}")
    st.write(f"TOTAL KG  :   {total_kg:.0f}")
    
    styled_df = df_sin_tenido.style.apply(highlight_mofijado, axis=1)
    # Aplicar formato con un decimal a la columna KG
    styled_df = df_sin_tenido.style.apply(highlight_mofijado, axis=1).format({"KG": "{:.1f}"})
    
    st.write(styled_df, unsafe_allow_html=True)
    

# Selección de días para la segunda consulta
dias_con_tenido = st.number_input("Días entre TEÑIDO y el día actual (por defecto 5) Partidas que no llevan estampado", min_value=1, value=5)
if st.button("Mostrar partidas TEÑIDAS pero no APROBADAS"):
    df_con_tenido = get_partidas_con_tenido_sin_aprob_tela(dias_con_tenido)

    # Contar los registros y sumar los KG
    total_registros = len(df_con_tenido)
    total_kg = df_con_tenido['KG'].sum()
    
    st.write(f"TOTAL REGISTROS  :   {total_registros}")
    st.write(f"TOTAL KG  :   {total_kg:.0f}")
    
    st.write(df_con_tenido)

# Selección de días para la tercera consulta
dias_con_tenido_estamp = st.number_input("Días entre TEÑIDO y el día actual (por defecto 5) Partidas que llevan estampado", min_value=1, value=20)
if st.button("Mostrar partidas TEÑIDAS (estamp) pero no APROBADAS"):
    df_con_tenido_estamp = get_partidas_con_tenido_sin_aprob_tela_estamp(dias_con_tenido_estamp)

    # Contar los registros y sumar los KG
    total_registros = len(df_con_tenido_estamp)
    total_kg = df_con_tenido_estamp['KG'].sum()
    
    st.write(f"TOTAL REGISTROS  :    {total_registros}")
    
    st.write(f"TOTAL KG  :   {total_kg:.0f}")
    
    st.write(df_con_tenido_estamp)
