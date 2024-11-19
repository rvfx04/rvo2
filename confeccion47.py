import streamlit as st
import pandas as pd
import pyodbc
from datetime import datetime

st.set_page_config(page_title="Confeccion 47")


# Configuración de la conexión a la base de datos
def get_connection():
    conn = pyodbc.connect(
        "driver={odbc driver 17 for sql server};"
        "server=" + st.secrets["server"] + ";"
        "database=" + st.secrets["database"] + ";"
        "uid=" + st.secrets["username"] + ";"
        "pwd=" + st.secrets["password"] + ";"
    )
    return conn        

# Consultas SQL
query_enviado = """
select c.CoddocOrdenProduccion AS OP, min(A.dtFechaRegistro) as FECHA,
    min(d.NommaeAnexoProveedor) AS PROVEEDOR, sum(b.dCantidadSal) AS UNIDADES
from docNotaInventario a 
inner join docNotaInventarioItem b on a.IdDocumento_NotaInventario = b.IdDocumento_NotaInventario
inner join docOrdenProduccion c on a.IdDocumento_OrdenProduccion = c.IdDocumento_OrdenProduccion
inner join maeAnexoProveedor d on a.IdmaeAnexo = d.IdmaeAnexo_Proveedor
where a.IdmaeAnexo IN (248,5526, 321, 5512,60,64)
    and a.dtFechaRegistro > '01-10-2024'
    and a.IdtdDocumentoForm = 130
    and a.bAnulado= 0
    and c.IdmaeAnexo_Cliente = 2533
    and b.dCantidadSal > 0
    and a.IdmaeSunatCTipoComprobantePago = 10
    and a.IdmaeTransaccionNota = 17
GROUP BY c.CoddocOrdenProduccion
"""

query_retornado = """
select c.CoddocOrdenProduccion AS OP, MIN(A.dtFechaRegistro) as FECHA,
    MIN(d.NommaeAnexoProveedor) AS PROVEEDOR, SUM(b.dCantidadIng) AS TOTAL_UNIDADES
from docNotaInventario a 
inner join docNotaInventarioItem b on a.IdDocumento_NotaInventario = b.IdDocumento_NotaInventario
inner join docOrdenProduccion c on a.IdDocumento_OrdenProduccion = c.IdDocumento_OrdenProduccion
inner join maeAnexoProveedor d on a.IdmaeAnexo = d.IdmaeAnexo_Proveedor
where a.IdmaeAnexo IN (248,5526)
    and a.dtFechaRegistro > '01-10-2024'
    and a.IdtdDocumentoForm = 131
    and a.bAnulado= 0
    and a.IdmaeSunatCTipoComprobantePago = 10
    and c.IdmaeAnexo_Cliente = 2533
GROUP BY c.CoddocOrdenProduccion
"""

# Función para ejecutar consultas y obtener DataFrames
def get_dataframe(query):
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Función para formatear fechas
def format_date(date):
    if pd.isnull(date):
        return ''
    return datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S').strftime('%d-%b').upper()

# Obtener DataFrames
df_enviado = get_dataframe(query_enviado)
df_retornado = get_dataframe(query_retornado)

# Formatear fechas
df_enviado['FECHA'] = df_enviado['FECHA'].apply(format_date)
df_retornado['FECHA'] = df_retornado['FECHA'].apply(format_date)

# Crear tabla resumen por proveedor
df_resumen = df_enviado.groupby('PROVEEDOR')['UNIDADES'].sum().reset_index()
df_resumen = df_resumen.rename(columns={'UNIDADES': 'ENVIADO'})

df_resumen_retorno = df_retornado.groupby('PROVEEDOR')['TOTAL_UNIDADES'].sum().reset_index()
df_resumen_retorno = df_resumen_retorno.rename(columns={'TOTAL_UNIDADES': 'RETORNADO'})

df_resumen = pd.merge(df_resumen, df_resumen_retorno, on='PROVEEDOR', how='outer').fillna(0)
df_resumen['SALDO'] = df_resumen['ENVIADO'] - df_resumen['RETORNADO']

# Agregar totales
totales = pd.DataFrame({
    'PROVEEDOR': ['TOTAL'],
    'ENVIADO': [df_resumen['ENVIADO'].sum()],
    'RETORNADO': [df_resumen['RETORNADO'].sum()],
    'SALDO': [df_resumen['SALDO'].sum()]
})
df_resumen = pd.concat([df_resumen, totales], ignore_index=True)

# Crear tabla detallada por OP
df_detalle = pd.merge(df_enviado, df_retornado, on=['OP', 'PROVEEDOR'], how='outer', suffixes=('_enviado', '_retornado'))
df_detalle = df_detalle.fillna({'UNIDADES': 0, 'TOTAL_UNIDADES': 0})
df_detalle['SALDO'] = df_detalle['UNIDADES'] - df_detalle['TOTAL_UNIDADES']
df_detalle = df_detalle.rename(columns={
    'FECHA_enviado': 'FECHA_ENVIO',
    'FECHA_retornado': 'FECHA_RETORNO',
    'UNIDADES': 'ENVIADO',
    'TOTAL_UNIDADES': 'RETORNADO'
})
df_detalle = df_detalle[['OP', 'FECHA_ENVIO', 'FECHA_RETORNO', 'PROVEEDOR', 'ENVIADO', 'RETORNADO', 'SALDO']]

# Mostrar resultados en Streamlit
st.title("47 B: Confección OP")

st.header("Resumen por Proveedor")
st.dataframe(df_resumen)

st.header("Detalle por OP")
st.dataframe(df_detalle)

# Opción para descargar los datos
st.download_button(
    label="Descargar resumen por proveedor (CSV)",
    data=df_resumen.to_csv(index=False).encode('utf-8'),
    file_name="resumen_proveedor.csv",
    mime="text/csv",
)

st.download_button(
    label="Descargar detalle por OP (CSV)",
    data=df_detalle.to_csv(index=False).encode('utf-8'),
    file_name="detalle_op.csv",
    mime="text/csv",
)
