import streamlit as st
import pandas as pd
import pyodbc
import base64
import io

# Función para conectar a la base de datos
def connect_to_database():
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=" + st.secrets["server"] + ";"
            "DATABASE=" + st.secrets["database"] + ";"
            "UID=" + st.secrets["username"] + ";"
            "PWD=" + st.secrets["password"] + ";"
        )
        return conn
    except Exception as e:
        st.error(f"Error al conectar a la base de datos: {e}")
        return None

# Función para ejecutar la consulta SQL y obtener resultados
def get_sql_data(conn, op):
    query = f"SELECT coddocordenproduccion, dcantidadprogramado FROM docordenproduccion WHERE coddocordenproduccion = '{op}'"
    df = pd.read_sql(query, conn)
    return df

# Aplicación Streamlit
def main():
    st.title("Carga de archivo excel, agrega una columna con info de BD y descarga del excel actualizado")

    uploaded_file = st.file_uploader("Subir archivo Excel", type=["xlsx"])

    if uploaded_file is not None:
        excel_data = pd.read_excel(uploaded_file)
        conn = connect_to_database()

        new_data = excel_data.copy()
       
        
        for index, row in new_data.iterrows():
            op = row['op']
            sql_data = get_sql_data(conn, op)
            if not sql_data.empty:
                new_data.at[index, 'dcantidadprogramado'] = sql_data['dcantidadprogramado'].values[0]

        st.write(new_data)

        st.write("Descarga el archivo Excel actualizado:")
        #st.dataframe(new_data)

        # Convertir el DataFrame a formato CSV en memoria (como una cadena de texto)
        csv = new_data.to_csv(index=False)

        # Generar el enlace de descarga del archivo CSV utilizando st.markdown
        b64 = base64.b64encode(csv.encode()).decode()  # Codifica el CSV en base64
        href = f'<a href="data:file/csv;base64,{b64}" download="excel_actualizado.csv">Descargar archivo CSV</a>'
        st.markdown(href, unsafe_allow_html=True)
        
        # Convertir el DataFrame a un archivo Excel en memoria (como una cadena de bytes)
        excel_bytes = None
        with io.BytesIO() as buffer:
            excel_writer = pd.ExcelWriter(buffer, engine='xlsxwriter')
            new_data.to_excel(excel_writer, index=False)
            excel_writer.close()  # Cerrar el escritor de Excel
            excel_bytes = buffer.getvalue()

        # Generar el enlace de descarga del archivo Excel utilizando st.markdown
        b64 = base64.b64encode(excel_bytes).decode()
        href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="excel_actualizado.xlsx">Descargar archivo Excel</a>'
        st.markdown(href, unsafe_allow_html=True)

if __name__ == '__main__':
    main()
