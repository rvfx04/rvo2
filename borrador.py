import streamlit as st
import pyodbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import psycopg2

st.set_page_config(layout="wide")

# Configurar la conexión a la base de datos utilizando las credenciales almacenadas en secrets
def connect_db():
    connection = pyodbc.connect(
        "driver={odbc driver 17 for sql server};"
        "server=" + st.secrets["msserver"] + ";"
        "database=" + st.secrets["msdatabase"] + ";"
        "uid=" + st.secrets["msusername"] + ";"
        "pwd=" + st.secrets["mspassword"] + ";"
    )
    return connection

def connect_postgres():
    connection = psycopg2.connect(
        host=st.secrets["host"],
        port=st.secrets["port"],
        database=st.secrets["database"],
        user=st.secrets["user"],
        password=st.secrets["password"]
    )
    return connection

def run_query(pedido):
    conn = connect_db()
    query = """[Tu consulta SQL original aquí]"""  # Mantener la consulta original

    df = pd.read_sql(query, conn, params=(pedido,))
    conn.close()
    return df

def run_postgres_query(pedido):
    conn = connect_postgres()
    
    query = '''[Tu consulta PostgreSQL original aquí]'''  # Mantener la consulta original
    
    df = pd.read_sql(query, conn, params=(pedido,))
    conn.close()
    return df

def procesar_pedido(pedido):
    try:
        # Execute SQL Server query
        df = run_query(pedido)
        
        # Execute PostgreSQL query
        df_postgres = run_postgres_query(pedido)
        
        if df.empty or df_postgres.empty:
            st.warning(f"No se encontraron datos para el pedido {pedido}.")
            return None

        # Procesar los datos para el gráfico de Gantt
        f_emision = pd.to_datetime(df['F_EMISION'].iloc[0])
        
        start_armado = pd.to_datetime(df_postgres['star_armado'].iloc[0])
        start_tenido = pd.to_datetime(df_postgres['star_tenido'].iloc[0])
        start_telaprob = pd.to_datetime(df_postgres['star_telaprob'].iloc[0])
        start_corte = pd.to_datetime(df_postgres['star_corte'].iloc[0])
        start_costura = pd.to_datetime(df_postgres['star_costura'].iloc[0])
        
        finish_armado = pd.to_datetime(df_postgres['finish_armado'].iloc[0])
        finish_tenido = pd.to_datetime(df_postgres['finish_tenido'].iloc[0])
        finish_telaprob = pd.to_datetime(df_postgres['finish_telaprob'].iloc[0])
        finish_corte = pd.to_datetime(df_postgres['finish_corte'].iloc[0])
        finish_costura = pd.to_datetime(df_postgres['finish_costura'].iloc[0])
        
        inicial = pd.to_datetime(df_postgres['Fecha_Colocacion'].iloc[0])
        fin = pd.to_datetime(df_postgres['Fecha_Entrega'].iloc[0])

        # Crear DataFrame para el gráfico de Gantt
        df_gantt = pd.DataFrame({
            'Pedido': pedido,
            'Proceso': ['ARMADO', 'TEÑIDO', 'TELA_APROB', 'CORTE', 'COSTURA'],
            'Start': [start_armado, start_tenido, start_telaprob, start_corte, start_costura],
            'Finish': [finish_armado, finish_tenido, finish_telaprob, finish_corte, finish_costura],
            'Start Real': [pd.to_datetime(df['FMINARM'].iloc[0]), 
                           pd.to_datetime(df['FMINTENID'].iloc[0]), 
                           pd.to_datetime(df['FMINTELAPROB'].iloc[0]), 
                           pd.to_datetime(df['FMINCORTE'].iloc[0]), 
                           pd.to_datetime(df['FMINCOSIDO'].iloc[0])],
            'Finish Real': [pd.to_datetime(df['FMAXARM'].iloc[0]), 
                            pd.to_datetime(df['FMAXTENID'].iloc[0]), 
                            pd.to_datetime(df['FMAXTELAPROB'].iloc[0]), 
                            pd.to_datetime(df['FMAXCORTE'].iloc[0]), 
                            pd.to_datetime(df['FMAXCOSIDO'].iloc[0])],
            'Avance': [df['KG_ARMP'].iloc[0], df['KG_TENIDP'].iloc[0], 
                       df['KG_TELAPROBP'].iloc[0], 
                       df['CORTADOP'].iloc[0], df['COSIDOP'].iloc[0]],
            'Cliente': df['CLIENTE'].iloc[0]
        })

        return df_gantt, inicial, fin, f_emision, df['F_ENTREGA'].iloc[0]

    except Exception as e:
        st.error(f"Error al procesar el pedido {pedido}: {e}")
        return None

def main():
    st.title("Diagrama de Gantt - Múltiples Pedidos")

    # Inicializar la sesión para guardar pedidos
    if 'pedidos_procesados' not in st.session_state:
        st.session_state.pedidos_procesados = []

    # Sidebar para ingresar pedidos
    with st.sidebar:
        st.header("Agregar Pedidos")
        nuevo_pedido = st.text_input("Número de Pedido")
        
        if st.button("Agregar Pedido"):
            if nuevo_pedido and nuevo_pedido not in st.session_state.pedidos_procesados:
                st.session_state.pedidos_procesados.append(nuevo_pedido)
                st.success(f"Pedido {nuevo_pedido} agregado")
            elif nuevo_pedido in st.session_state.pedidos_procesados:
                st.warning("Este pedido ya ha sido agregado")

        if st.button("Limpiar Pedidos"):
            st.session_state.pedidos_procesados = []

    # Procesar y mostrar gráfico de Gantt
    if st.session_state.pedidos_procesados:
        # Recopilar datos de todos los pedidos
        datos_gantt = []
        min_fecha = datetime.max
        max_fecha = datetime.min

        for pedido in st.session_state.pedidos_procesados:
            resultado = procesar_pedido(pedido)
            if resultado:
                df_gantt, inicial, fin, f_emision, f_entrega = resultado
                datos_gantt.append(df_gantt)
                
                min_fecha = min(min_fecha, inicial)
                max_fecha = max(max_fecha, fin)

        # Concatenar dataframes de Gantt
        df_gantt_total = pd.concat(datos_gantt)

        # Crear gráfico de Gantt
        fig = px.timeline(df_gantt_total, 
                          x_start="Start", 
                          x_end="Finish", 
                          y="Proceso", 
                          color="Pedido",
                          text="Avance",
                          hover_data=['Cliente'])

        # Ajustar el diseño del gráfico
        fig.update_yaxes(autorange="reversed")
        fig.update_xaxes(range=[min_fecha, max_fecha])

        # Personalizar el layout
        fig.update_layout(
            title="Diagrama de Gantt de Múltiples Pedidos",
            xaxis_title="Cronograma",
            yaxis_title="Procesos",
            height=600
        )

        st.plotly_chart(fig, use_container_width=True)

        # Mostrar tabla de resumen
        st.subheader("Resumen de Pedidos")
        st.dataframe(df_gantt_total[['Pedido', 'Cliente', 'Proceso', 'Start', 'Finish', 'Avance']])

    else:
        st.info("Agrega pedidos en la barra lateral para generar el diagrama de Gantt")

if __name__ == '__main__':
    main()
