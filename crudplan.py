import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import sql

class PostgreSQLApp:
    def __init__(self):
        # Initialize connection parameters from Streamlit secrets
        self.host = st.secrets["host"]
        self.port = st.secrets["port"]
        self.database = st.secrets["database"]
        self.user = st.secrets["user"]
        self.password = st.secrets["password"]

    def _get_connection(self):
        """Establishes a connection to PostgreSQL database"""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )

    def execute_query(self, query, params=None):
        """Execute a query and return results as a DataFrame"""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                if params:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
                
                # Commit for write operations
                if query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')):
                    conn.commit()
                
                # Check if the query is a SELECT
                if query.strip().upper().startswith('SELECT'):
                    columns = [desc[0] for desc in cur.description]
                    return pd.DataFrame(cur.fetchall(), columns=columns)

    def read_records(self, id_filter=None):
        """Read records from the database, optionally filtered by ID"""
        base_query = 'SELECT * FROM "docOrdenVenta"'
        
        if id_filter:
            query = base_query + f' WHERE "IdDocumento_OrdenVenta" = %s'
            return self.execute_query(query, (id_filter,))
        else:
            return self.execute_query(base_query)

    def create_record(self, record_data):
        """Insert a new record into the database"""
        query = sql.SQL("""
            INSERT INTO "docOrdenVenta" (
                "IdDocumento_OrdenVenta",
                "Fecha_Entrega",
                "Fecha_Colocacion",
                "star_armado",
                "finish_armado",
                "star_tenido",
                "finish_tenido",
                "star_proc_tela",
                "finish_proc_tela",
                "star_telaprob",
                "finish_telaprob",
                "star_corte",
                "finish_corte",
                "star_costura",
                "finish_costura",
                "star_proc_prenda",
                "finish_proc_prenda"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)
        
        try:
            self.execute_query(query, (
                record_data['IdDocumento_OrdenVenta'],
                record_data['Fecha_Entrega'],
                record_data['Fecha_Colocacion'],
                record_data['star_armado'],
                record_data['finish_armado'],
                record_data['star_tenido'],
                record_data['finish_tenido'],
                record_data['star_proc_tela'],
                record_data['finish_proc_tela'],
                record_data['star_telaprob'],
                record_data['finish_telaprob'],
                record_data['star_corte'],
                record_data['finish_corte'],
                record_data['star_costura'],
                record_data['finish_costura'],
                record_data['star_proc_prenda'],
                record_data['finish_proc_prenda']
            ))
            st.success("Registro creado exitosamente")
        except Exception as e:
            st.error(f"Error al crear el registro: {e}")

    def update_record(self, record_data):
        """Update an existing record in the database"""
        query = sql.SQL("""
            UPDATE "docOrdenVenta"
            SET "Fecha_Entrega" = %s,
                "Fecha_Colocacion" = %s,
                "star_armado" = %s,
                "finish_armado" = %s,
                "star_tenido" = %s,
                "finish_tenido" = %s,
                "star_proc_tela" = %s,
                "finish_proc_tela" = %s,
                "star_telaprob" = %s,
                "finish_telaprob" = %s,
                "star_corte" = %s,
                "finish_corte" = %s,
                "star_costura" = %s,
                "finish_costura" = %s,
                "star_proc_prenda" = %s,
                "finish_proc_prenda" = %s
            WHERE "IdDocumento_OrdenVenta" = %s
        """)
        
        try:
            self.execute_query(query, (
                record_data['Fecha_Entrega'],
                record_data['Fecha_Colocacion'],
                record_data['star_armado'],
                record_data['finish_armado'],
                record_data['star_tenido'],
                record_data['finish_tenido'],
                record_data['star_proc_tela'],
                record_data['finish_proc_tela'],
                record_data['star_telaprob'],
                record_data['finish_telaprob'],
                record_data['star_corte'],
                record_data['finish_corte'],
                record_data['star_costura'],
                record_data['finish_costura'],
                record_data['star_proc_prenda'],
                record_data['finish_proc_prenda'],
                record_data['IdDocumento_OrdenVenta']
            ))
            st.success("Registro actualizado exitosamente")
        except Exception as e:
            st.error(f"Error al actualizar el registro: {e}")

    def delete_record(self, record_id):
        """Delete a record from the database"""
        query = 'DELETE FROM "docOrdenVenta" WHERE "IdDocumento_OrdenVenta" = %s'
        
        try:
            self.execute_query(query, (record_id,))
            st.success("Registro eliminado exitosamente")
        except Exception as e:
            st.error(f"Error al eliminar el registro: {e}")

def main():
    st.title("Gestión de Órdenes de Venta")
    
    # Initialize the PostgreSQL app
    pg_app = PostgreSQLApp()
    
    # Create tabs for different operations
    tab1, tab2, tab3, tab4 = st.tabs([
        "Consultar", 
        "Crear Registro", 
        "Actualizar Registro", 
        "Eliminar Registro"
    ])
    
    with tab1:
        st.header("Consultar Registros")
        search_id = st.text_input("Ingrese el ID del pedido a buscar:")
        
        if st.button("Buscar"):
            result = pg_app.read_records(search_id if search_id else None)
            
            if not result.empty:
                st.dataframe(result, hide_index=True)
            else:
                st.write("No se encontraron resultados")
    
    with tab2:
        st.header("Crear Nuevo Registro")
        with st.form("create_record"):
            id_doc = st.text_input("ID Documento")
            fecha_entrega = st.date_input("Fecha de Entrega")
            fecha_colocacion = st.date_input("Fecha de Colocación")
            star_armado = st.text_input("Inicio Armado")
            finish_armado = st.text_input("Fin Armado")
            star_tenido = st.text_input("Inicio Teñido")
            finish_tenido = st.text_input("Fin Teñido")
            star_proc_tela = st.text_input("Inicio Proceso de Tela")
            finish_proc_tela = st.text_input("Fin Proceso de Tela")
            star_telaprob = st.text_input("Inicio Prueba de Tela")
            finish_telaprob = st.text_input("Fin Prueba de Tela")
            star_corte = st.text_input("Inicio Corte")
            finish_corte = st.text_input("Fin Corte")
            star_costura = st.text_input("Inicio Costura")
            finish_costura = st.text_input("Fin Costura")
            star_proc_prenda = st.text_input("Inicio Proceso de Prenda")
            finish_proc_prenda = st.text_input("Fin Proceso de Prenda")
            
            submit_create = st.form_submit_button("Crear Registro")
            
            if submit_create:
                record_data = {
                    'IdDocumento_OrdenVenta': id_doc,
                    'Fecha_Entrega': fecha_entrega,
                    'Fecha_Colocacion': fecha_colocacion,
                    'star_armado': star_armado,
                    'finish_armado': finish_armado,
                    'star_tenido': star_tenido,
                    'finish_tenido': finish_tenido,
                    'star_proc_tela': star_proc_tela,
                    'finish_proc_tela': finish_proc_tela,
                    'star_telaprob': star_telaprob,
                    'finish_telaprob': finish_telaprob,
                    'star_corte': star_corte,
                    'finish_corte': finish_corte,
                    'star_costura': star_costura,
                    'finish_costura': finish_costura,
                    'star_proc_prenda': star_proc_prenda,
                    'finish_proc_prenda': finish_proc_prenda
                }
                pg_app.create_record(record_data)
    
    with tab3:
        st.header("Actualizar Registro")
        with st.form("update_record"):
            id_update = st.text_input("ID Documento a Actualizar")
            fecha_entrega_update = st.date_input("Nueva Fecha de Entrega")
            fecha_colocacion_update = st.date_input("Nueva Fecha de Colocación")
            star_armado_update = st.text_input("Nuevo Inicio Armado")
            finish_armado_update = st.text_input("Nuevo Fin Armado")
            star_tenido_update = st.text_input("Nuevo Inicio Teñido")
            finish_tenido_update = st.text_input("Nuevo Fin Teñido")
            star_proc_tela_update = st.text_input("Nuevo Inicio Proceso de Tela")
            finish_proc_tela_update = st.text_input("Nuevo Fin Proceso de Tela")
            star_telaprob_update = st.text_input("Nuevo Inicio Prueba de Tela")
            finish_telaprob_update = st.text_input("Nuevo Fin Prueba de Tela")
            star_corte_update = st.text_input("Nuevo Inicio Corte")
            finish_corte_update = st.text_input("Nuevo Fin Corte")
            star_costura_update = st.text_input("Nuevo Inicio Costura")
            finish_costura_update = st.text_input("Nuevo Fin Costura")
            star_proc_prenda_update = st.text_input("Nuevo Inicio Proceso de Prenda")
            finish_proc_prenda_update = st.text_input("Nuevo Fin Proceso de Prenda")
            
            submit_update = st.form_submit_button("Actualizar Registro")
            
            if submit_update:
                record_data = {
                    'IdDocumento_OrdenVenta': id_update,
                    'Fecha_Entrega': fecha_entrega_update,
                    'Fecha_Colocacion': fecha_colocacion_update,
                    'star_armado': star_armado_update,
                    'finish_armado': finish_armado_update,
                    'star_tenido': star_tenido_update,
                    'finish_tenido': finish_tenido_update,
                    'star_proc_tela': star_proc_tela_update,
                    'finish_proc_tela': finish_proc_tela_update,
                    'star_telaprob': star_telaprob_update,
                    'finish_telaprob': finish_telaprob_update,
                    'star_corte': star_corte_update,
                    'finish_corte': finish_corte_update,
                    'star_costura': star_costura_update,
                    'finish_costura': finish_costura_update,
                    'star_proc_prenda': star_proc_prenda_update,
                    'finish_proc_prenda': finish_proc_prenda_update
                }
                pg_app.update_record(record_data)
    
    with tab4:
        st.header("Eliminar Registro")
        delete_id = st.text_input("Ingrese el ID del documento a eliminar")
        
        if st.button("Eliminar"):
            pg_app.delete_record(delete_id)

if __name__ == "__main__":
    main()
