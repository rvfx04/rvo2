import streamlit as st
import pandas as pd
import plotly.express as px
import pyodbc
from datetime import datetime

# [Previous functions remain the same: connect_to_db(), run_query_enviadas(), run_query_regresadas(), safe_date_format()]

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
    
    # Agregar columna de PEDIDO
    df_detallado['PEDIDO'] = df_detallado['OP'].str[:4]
    
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
    col1, col2 = st.columns(2)
    
    # Selector de Proveedor
    with col1:
        proveedores_disponibles = ['Todos'] + sorted(df_detallado['PROVEEDOR'].unique().tolist())
        proveedor_seleccionado = st.selectbox('Filtrar por Proveedor:', proveedores_disponibles)
    
    # Selector de Pedido
    with col2:
        pedidos_disponibles = ['Todos'] + sorted(df_detallado['PEDIDO'].unique().tolist())
        pedido_seleccionado = st.selectbox('Filtrar por Pedido:', pedidos_disponibles)
    
    # Aplicar filtros
    df_filtrado = df_detallado.copy()
    if proveedor_seleccionado != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['PROVEEDOR'] == proveedor_seleccionado]
    
    if pedido_seleccionado != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['PEDIDO'] == pedido_seleccionado]

    # Seleccionar columnas para mostrar
    columns_to_display = ['OP', 'PEDIDO', 'PROVEEDOR', 'FECHA_ENVIO_FORMATTED', 'FECHA_REGRESO_FORMATTED', 
                          'UNIDADES_ENVIADAS', 'UNIDADES_REGRESADAS', 'SALDO']
    df_display = df_filtrado[columns_to_display]

    # Renombrar las columnas para la visualización
    df_display = df_display.rename(columns={
        'FECHA_ENVIO_FORMATTED': 'F_ENVIO',
        'FECHA_REGRESO_FORMATTED': 'F_REGRESO',
        'UNIDADES_ENVIADAS': 'U_ENV',
        'UNIDADES_REGRESADAS': 'U_REG',
        'PROVEEDOR': 'PROV'
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
