import streamlit as st
import pandas as pd
import pyodbc
import psycopg2


# Función para conectarse a SQL Server y ejecutar una consulta
def execute_sqlserver_query(query):
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=" + st.secrets["msserver"] + ";"
        "DATABASE=" + st.secrets["msdatabase"] + ";"
        "UID=" + st.secrets["msusername"] + ";"
        "PWD=" + st.secrets["mspassword"] + ";"
    )
    df = pd.read_sql(query, conn)
    conn.close()
    return df


# Función para conectarse a PostgreSQL y ejecutar una consulta
def execute_postgres_query(query):
    conn = psycopg2.connect(
        host=st.secrets["host"],
        port=st.secrets["port"],
        database=st.secrets["database"],
        user=st.secrets["user"],
        password=st.secrets["password"]
    )
    df = pd.read_sql(query, conn)
    conn.close()
    return df


# Título de la aplicación
st.title("Consulta de múltiples bases de datos")

# Campo de búsqueda
id = st.text_input("Ingrese el ID del pedido a buscar:")

# Consultas SQL base
sqlserver_base_query = """
SELECT
    IdmaeAnexo_Cliente AS ID,
    NommaeAnexoCliente AS CLIENTE
FROM maeAnexoCliente
"""

postgres_base_query = 'SELECT * FROM "docOrdenVenta"'

# Si hay un término de búsqueda, modificar las consultas
if id:
    sqlserver_query = sqlserver_base_query + f" WHERE IdmaeAnexo_Cliente = '{id}'"
    postgres_query = postgres_base_query + f' WHERE "IdDocumento_OrdenVenta" = \'{id}\''

    st.subheader("Resultados de la búsqueda")

    # Ejecutar búsqueda en SQL Server
    df_sqlserver_search = execute_sqlserver_query(sqlserver_query)
    if not df_sqlserver_search.empty:
        st.write("Resultado en SQL Server:")
        st.dataframe(df_sqlserver_search, hide_index=True)
    else:
        st.write("No se encontraron resultados en SQL Server")

    # Ejecutar búsqueda en PostgreSQL
    df_postgres_search = execute_postgres_query(postgres_query)
    if not df_postgres_search.empty:
        st.write("Resultado en PostgreSQL:")
        st.dataframe(df_postgres_search, hide_index=True)
    else:
        st.write("No se encontraron resultados en PostgreSQL")

# Mostrar todos los datos si no hay búsqueda
else:
    # Ejecutar las consultas base
    df_sqlserver = execute_sqlserver_query(sqlserver_base_query)
    df_postgres = execute_postgres_query(postgres_base_query)

    # Mostrar resultados de SQL Server
    st.subheader("Datos de SQL Server")
    st.write(f"Número de registros: {len(df_sqlserver)}")
    st.dataframe(df_sqlserver, hide_index=True)

    # Mostrar resultados de PostgreSQL
    st.subheader("Datos de PostgreSQL")
    st.write(f"Número de registros: {len(df_postgres)}")
    st.dataframe(df_postgres, hide_index=True)
