import streamlit as st
import pandas as pd
import plotly.express as px
import pyodbc
from datetime import datetime

# [Previous functions remain the same: connect_to_db(), run_query_enviadas(), run_query_regresadas(), safe_date_format()]

# Configuración de la página
st.set_page_config(page_title="Control de bordados 47")

# Función para conectar a la base de datos
def connect_to_db():
    connection = pyodbc.connect(
        "driver={odbc driver 17 for sql server};"
        "server=" + st.secrets["server"] + ";"
        "database=" + st.secrets["database"] + ";"
        "uid=" + st.secrets["username"] + ";"
        "pwd=" + st.secrets["password"] + ";"
    )
    return connection

# Función para ejecutar la consulta de unidades enviadas
def run_query_enviadas():
    conn = connect_to_db()
    query = """
    SELECT 
        e.CoddocOrdenProduccion AS OP,
        MIN(d.dtFechaEmision) AS FECHA_ENVIO,
        MIN(f.NommaeAnexoProveedor) AS PROVEEDOR,
        SUM(b.dCantidadSal) AS UNIDADES_ENVIADAS, b.IdDocumento_NotaInventario
    FROM docNotaInventarioItem b
    INNER JOIN docGuiaRemisionDetalle c ON b.IdDocumento_NotaInventario = c.IdDocumento_NotaInventario
    INNER JOIN docGuiaRemision d ON c.IdDocumento_GuiaRemision = d.IdDocumento_GuiaRemision
    INNER JOIN docNotaInventario a ON a.IdDocumento_NotaInventario = b.IdDocumento_NotaInventario
    INNER JOIN docOrdenProduccion e ON a.IdDocumento_OrdenProduccion = e.IdDocumento_OrdenProduccion
    INNER JOIN maeAnexoProveedor f ON f.IdmaeAnexo_Proveedor = d.IdmaeAnexo_Destino
    WHERE d.IdmaeAnexo_Destino IN (6536, 4251, 6546, 6626)
        AND b.dCantidadSal > 0
        AND c.IdtdDocumentoForm_NotaInventario = 130
        AND d.dtFechaEmision > '01-09-2024'
        AND a.bAnulado = 0
        AND d.bAnulado = 0 and NOT  a.IdDocumento_NotaInventario in (489353,493532,486774,493055,492058)
    GROUP BY e.CoddocOrdenProduccion, b.IdDocumento_NotaInventario
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Función para ejecutar la consulta de unidades regresadas
def run_query_regresadas():
    conn = connect_to_db()
    query = """
    SELECT c.CoddocOrdenProduccion AS OP, MIN(A.dtFechaRegistro) as FECHA_REGRESO,
        MIN(d.NommaeAnexoProveedor) AS PROVEEDOR, SUM(b.dCantidadIng) AS UNIDADES_REGRESADAS, a.IdDocumento_NotaInventario
    FROM docNotaInventario a 
    INNER JOIN docNotaInventarioItem b ON a.IdDocumento_NotaInventario = b.IdDocumento_NotaInventario
    INNER JOIN docOrdenProduccion c ON a.IdDocumento_OrdenProduccion = c.IdDocumento_OrdenProduccion
    INNER JOIN maeAnexoProveedor d ON a.IdmaeAnexo = d.IdmaeAnexo_Proveedor
    WHERE a.IdmaeAnexo IN (6536,4251, 6546)
        AND a.dtFechaRegistro > '01-09-2024'
        AND a.IdtdDocumentoForm = 131
        AND a.bAnulado = 0
        AND a.IdmaeSunatCTipoComprobantePago = 10 and NOT  a.IdDocumento_NotaInventario in (489353,493532,486774,493055,492058)
    GROUP BY c.CoddocOrdenProduccion , a.IdDocumento_NotaInventario
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Función para formatear fechas de manera segura
def safe_date_format(date):
    if pd.isnull(date):
        return ''
    if isinstance(date, (int, float)):
        try:
            return datetime.fromtimestamp(date).strftime('%Y-%m-%d')
        except:
            return str(date)
    if isinstance(date, str):
        try:
            return datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
        except:
            return date
    if isinstance(date, datetime):
        return date.strftime('%Y-%m-%d')
    return str(date)


# Título de la aplicación
st.title("Control de bordados 47")

# Cargar datos
try:
    df_enviadas = run_query_enviadas()
    df_regresadas = run_query_regresadas()

    # Convertir fechas de manera segura
    df_enviadas['FECHA_ENVIO'] = pd.to_datetime(df_enviadas['FECHA_ENVIO'], errors='coerce')
    df_regresadas['FECHA_REGRESO'] = pd.to_datetime(df_regresadas['FECHA_REGRESO'], errors='coerce')

    df_detallado = pd.merge(df_enviadas, df_regresadas, on=['OP', 'PROVEEDOR'], how='outer')

    # Llenar NaN con 0 solo para las columnas numéricas
    df_detallado['UNIDADES_ENVIADAS'] = df_detallado['UNIDADES_ENVIADAS'].fillna(0)
    df_detallado['UNIDADES_REGRESADAS'] = df_detallado['UNIDADES_REGRESADAS'].fillna(0)

    df_detallado['SALDO'] = df_detallado['UNIDADES_ENVIADAS'] - df_detallado['UNIDADES_REGRESADAS']
    
    # Agregar columna de PEDIDO y columna de estado de retorno
    df_detallado['PEDIDO'] = df_detallado['OP'].str[:4]
    df_detallado['ESTADO_RETORNO'] = df_detallado.apply(
        lambda row: 'Pendiente' if row['UNIDADES_REGRESADAS'] == 0 else 'Retornado', axis=1
    )
    
    # Ordenar por OP
    df_detallado = df_detallado.sort_values('OP')

    # Calcular totales
    total_enviadas = df_detallado['UNIDADES_ENVIADAS'].sum()
    total_regresadas = df_detallado['UNIDADES_REGRESADAS'].sum()
    saldo_total = total_enviadas - total_regresadas

    # Mostrar estadísticas
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Envio", f"{total_enviadas:,.0f}")
    col2.metric("Total Retorno", f"{total_regresadas:,.0f}")
    col3.metric("Saldo Total", f"{saldo_total:,.0f}")

    # [Previous sections remain the same]

    # Mostrar datos detallados combinados
    st.subheader("Datos Detallados por OP")
    
    # Aplicar el formato personalizado
    df_detallado['FECHA_ENVIO_FORMATTED'] = df_detallado['FECHA_ENVIO'].apply(safe_date_format)
    df_detallado['FECHA_REGRESO_FORMATTED'] = df_detallado['FECHA_REGRESO'].apply(lambda x: safe_date_format(x) if pd.notnull(x) else '')

    # Selector de filtros
    col1, col2, col3 = st.columns(3)
    
    # Selector de Proveedor
    with col1:
        proveedores_disponibles = ['Todos'] + sorted(df_detallado['PROVEEDOR'].unique().tolist())
        proveedor_seleccionado = st.selectbox('Filtrar por Proveedor:', proveedores_disponibles)
    
    # Selector de Pedido
    with col2:
        pedidos_disponibles = ['Todos'] + sorted(df_detallado['PEDIDO'].unique().tolist())
        pedido_seleccionado = st.selectbox('Filtrar por Pedido:', pedidos_disponibles)
    
    # Selector de Estado de Retorno
    with col3:
        estado_retorno_opciones = ['Todos', 'Pendiente', 'Retornado']
        estado_retorno_seleccionado = st.selectbox('Estado de Retorno:', estado_retorno_opciones)
    
    # Aplicar filtros
    df_filtrado = df_detallado.copy()
    if proveedor_seleccionado != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['PROVEEDOR'] == proveedor_seleccionado]
    
    if pedido_seleccionado != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['PEDIDO'] == pedido_seleccionado]
    
    if estado_retorno_seleccionado != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['ESTADO_RETORNO'] == estado_retorno_seleccionado]

    # Seleccionar columnas para mostrar
    columns_to_display = ['OP', 'PEDIDO', 'PROVEEDOR', 'FECHA_ENVIO_FORMATTED', 'FECHA_REGRESO_FORMATTED', 
                          'UNIDADES_ENVIADAS', 'UNIDADES_REGRESADAS', 'SALDO', 'ESTADO_RETORNO']
    df_display = df_filtrado[columns_to_display]

    # Renombrar las columnas para la visualización
    df_display = df_display.rename(columns={
        'FECHA_ENVIO_FORMATTED': 'F_ENVIO',
        'FECHA_REGRESO_FORMATTED': 'F_REGRESO',
        'UNIDADES_ENVIADAS': 'U_ENV',
        'UNIDADES_REGRESADAS': 'U_REG',
        'PROVEEDOR': 'PROV',
        'ESTADO_RETORNO': 'ESTADO'
    })

    st.dataframe(df_display.style.format({
        'U_ENV': '{:,.0f}',
        'U_REG': '{:,.0f}',
        'SALDO': '{:,.0f}',
        'F_ENVIO': '{}',
        'F_REGRESO': '{}'
    }))

except Exception as e:
    st.error(f"Ocurrió un error al cargar los datos: {str(e)}")
    st.error("Detalles del error:")
    st.exception(e)
