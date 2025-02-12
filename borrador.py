import streamlit as st
import pyodbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import psycopg2

# Configuración de la página
st.set_page_config(layout="wide")
st.title("Progreso de Pedidos Consolidado")

# Funciones de conexión a la base de datos
def connect_db(db_type='mssql'):
    """Conecta a la base de datos especificada."""
    if db_type == 'mssql':
        return pyodbc.connect(
            "driver={ODBC Driver 17 for SQL Server};"
            "server=" + st.secrets["msserver"] + ";"
            "database=" + st.secrets["msdatabase"] + ";"
            "uid=" + st.secrets["msusername"] + ";"
            "pwd=" + st.secrets["mspassword"] + ";"
        )
    elif db_type == 'postgres':
        return psycopg2.connect(
            host=st.secrets["host"],
            port=st.secrets["port"],
            database=st.secrets["database"],
            user=st.secrets["user"],
            password=st.secrets["password"]
        )
    else:
        raise ValueError("Tipo de base de datos no soportado.")

# Función para agregar filas de resumen
def add_summary_row(df, db_type='mssql'):
    """Agrega una fila de resumen al DataFrame."""
    if db_type == 'mssql':
        summary = pd.Series({
            'PEDIDO': 'RESUMEN',
            'F_EMISION': df['F_EMISION'].min(),
            'F_ENTREGA': df['F_ENTREGA'].min(),
            'DIAS': None,
            'CLIENTE': 'TOTAL',
            'PO': 'TOTAL',
            'KG_REQ': df['KG_REQ'].sum(),
            'UNID': df['UNID'].sum(),
        })
        # Cálculos adicionales para MSSQL
        total_kg = df['KG_REQ'].sum()
        for col in ['KG_ARMP', 'KG_TENIDP', 'KG_TELAPROBP']:
            weighted_sum = sum(
                float(str(row[col]).rstrip('%')) * row['KG_REQ']
                for _, row in df.iterrows()
            )
            summary[col] = f"{(weighted_sum / total_kg if total_kg > 0 else 0):.0f}%"
        
        total_units = df['UNID'].sum()
        for col in ['PROGP', 'CORTADOP', 'COSIDOP']:
            weighted_sum = sum(
                float(str(row[col]).rstrip('%')) * row['UNID']
                for _, row in df.iterrows()
            )
            summary[col] = f"{(weighted_sum / total_units if total_units > 0 else 0):.0f}%"
        
        # Fechas mínimas y máximas
        min_date_cols = ['FMINARM', 'FMINTENID', 'FMINTELAPROB', 'FMINCORTE', 'FMINCOSIDO']
        max_date_cols = ['FMAXARM', 'FMAXTENID', 'FMAXTELAPROB', 'FMAXCORTE', 'FMAXCOSIDO']
        for col in min_date_cols:
            summary[col] = df[col].min()
        for col in max_date_cols:
            summary[col] = df[col].max()
    
    elif db_type == 'postgres':
        summary = pd.Series({
            'pedido': 'RESUMEN',
            'Fecha_Colocacion': df['Fecha_Colocacion'].min(),
            'Fecha_Entrega': df['Fecha_Entrega'].min(),
        })
        # Fechas mínimas y máximas para PostgreSQL
        start_cols = ['star_armado', 'star_tenido', 'star_telaprob', 'star_corte', 'star_costura']
        finish_cols = ['finish_armado', 'finish_tenido', 'finish_telaprob', 'finish_corte', 'finish_costura']
        for col in start_cols:
            summary[col] = df[col].min()
        for col in finish_cols:
            summary[col] = df[col].max()
    
    else:
        raise ValueError("Tipo de base de datos no soportado.")
    
    df_with_summary = pd.concat([df, pd.DataFrame([summary])], ignore_index=True)
    return df_with_summary

# Función para ejecutar consultas (usando st.cache_data)
@st.cache_data
def run_query(pedidos, db_type='mssql'):
    """Ejecuta una consulta en la base de datos especificada."""
    conn = connect_db(db_type)
    if db_type == 'mssql':
        query = """
        SELECT gg.PEDIDO, gg.F_EMISION, gg.F_ENTREGA, gg.DIAS, gg.CLIENTE, gg.PO, gg.KG_REQ, 
               gg.KG_ARMP, gg.KG_TENIDP, gg.KG_TELAPROBP, gg.UNID, gg.PROGP, gg.CORTADOP, gg.COSIDOP, 
               ff.FMINARM, ff.FMAXARM, ff.FMINTENID, ff.FMAXTENID, ff.FMINTELAPROB, ff.FMAXTELAPROB, ff.FMINCORTE, ff.FMAXCORTE, ff.FMINCOSIDO, ff.FMAXCOSIDO
        FROM 
            (SELECT ... ) gg
        INNER JOIN 
            (SELECT ... ) ff
        ON gg.IdDocumento_OrdenVenta = ff.IdDocumento_OrdenVenta
        WHERE gg.PEDIDO IN ({})
        """.format(','.join(['?' for _ in pedidos]))
    elif db_type == 'postgres':
        query = """
        SELECT 
            "IdDocumento_OrdenVenta" as pedido,
            "Fecha_Colocacion",
            "Fecha_Entrega",
            "star_armado",
            "star_tenido",
            "star_telaprob",
            "star_corte",
            "star_costura",
            "finish_armado",
            "finish_tenido",
            "finish_telaprob",
            "finish_corte",
            "finish_costura"
        FROM "docOrdenVenta"
        WHERE "IdDocumento_OrdenVenta" IN ({})
        """.format(','.join(['%s' for _ in pedidos]))
    else:
        raise ValueError("Tipo de base de datos no soportado.")
    
    df = pd.read_sql(query, conn, params=tuple(pedidos))
    conn.close()
    return df

# Función para crear el gráfico de Gantt
def create_gantt_chart(df, df_postgres):
    """Crea un gráfico de Gantt con los datos proporcionados."""
    n = len(df) - 1  # Índice de la fila de resumen
    
    # Fechas de inicio y fin
    start_armado = pd.to_datetime(df_postgres['star_armado'].iloc[n])
    finish_armado = pd.to_datetime(df_postgres['finish_armado'].iloc[n])
    start_tenido = pd.to_datetime(df_postgres['star_tenido'].iloc[n])
    finish_tenido = pd.to_datetime(df_postgres['finish_tenido'].iloc[n])
    start_telaprob = pd.to_datetime(df_postgres['star_telaprob'].iloc[n])
    finish_telaprob = pd.to_datetime(df_postgres['finish_telaprob'].iloc[n])
    start_corte = pd.to_datetime(df_postgres['star_corte'].iloc[n])
    finish_corte = pd.to_datetime(df_postgres['finish_corte'].iloc[n])
    start_costura = pd.to_datetime(df_postgres['star_costura'].iloc[n])
    finish_costura = pd.to_datetime(df_postgres['finish_costura'].iloc[n])
    
    # Crear DataFrame para el gráfico de Gantt
    df_gantt = pd.DataFrame({
        'Proceso': ['ARMADO', 'TEÑIDO', 'TELA_APROB', 'CORTE', 'COSTURA'],
        'Start': [start_armado, start_tenido, start_telaprob, start_corte, start_costura],
        'Finish': [finish_armado, finish_tenido, finish_telaprob, finish_corte, finish_costura],
        'Avance': [df['KG_ARMP'].iloc[n], df['KG_TENIDP'].iloc[n], df['KG_TELAPROBP'].iloc[n], 
                   df['CORTADOP'].iloc[n], df['COSIDOP'].iloc[n]]
    })
    
    # Crear el gráfico de Gantt
    fig = px.timeline(df_gantt, x_start="Start", x_end="Finish", y="Proceso", text="Avance")
    fig.update_xaxes(tickmode='linear', dtick=2 * 24 * 60 * 60 * 1000, tickformat='%d\n%b\n%y')
    fig.update_yaxes(autorange="reversed")
    
    return fig

# Interfaz de usuario
pedidos_input = st.text_input("Ingresa los números de pedido (separados por coma)")

if st.button("Ejecutar Consulta"):
    if pedidos_input:
        try:
            pedidos = [p.strip() for p in pedidos_input.split(',')]
            
            # Ejecutar consultas
            df = run_query(pedidos, db_type='mssql')
            df_postgres = run_query(pedidos, db_type='postgres')
            
            if df.empty:
                st.warning("No se encontraron datos para estos pedidos en SQL Server.")
            else:
                # Agregar filas de resumen
                df = add_summary_row(df, db_type='mssql')
                df_postgres = add_summary_row(df_postgres, db_type='postgres')
                
                # Mostrar datos detallados
                st.subheader("Detalle por Pedido")
                st.dataframe(df)
                
                st.subheader("Info Plan")
                st.dataframe(df_postgres)
                
                # Crear y mostrar el gráfico de Gantt
                fig = create_gantt_chart(df, df_postgres)
                st.plotly_chart(fig)
        except Exception as e:
            st.error(f"Error al ejecutar la consulta: {e}")
    else:
        st.warning("Por favor ingresa un número de pedido.")
