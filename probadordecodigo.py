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
            (SELECT
                a.CoddocOrdenVenta AS PEDIDO, 
                a.IdDocumento_OrdenVenta,
                CASE WHEN ISDATE(a.dtFechaEmision) = 1 THEN CONVERT(DATE, a.dtFechaEmision) ELSE NULL END AS F_EMISION,
                CASE WHEN ISDATE(a.dtFechaEntrega) = 1 THEN CONVERT(DATE, a.dtFechaEntrega) ELSE NULL END AS F_ENTREGA,
                DATEDIFF(day, a.dtFechaEmision, a.dtFechaEntrega) AS DIAS,
                SUBSTRING(b.NommaeAnexoCliente, 1, 15) AS CLIENTE,
                a.nvDocumentoReferencia AS PO,
                CONVERT(INT, COALESCE(d.KG, 0)) AS KG_REQ,
                FORMAT(CASE WHEN d.KG = 0 THEN 0 ELSE (COALESCE(t.KG_ARM, 0) / d.KG) END, '0%') AS KG_ARMP,
                FORMAT(CASE WHEN d.KG = 0 THEN 0 ELSE (COALESCE(t.KG_TEÑIDOS, 0) / d.KG) END, '0%') AS KG_TENIDP,
                FORMAT(CASE WHEN d.KG = 0 THEN 0 ELSE (COALESCE(t.KG_PRODUC, 0) / d.KG) END, '0%') AS KG_TELAPROBP,
                CONVERT(INT, a.dCantidad) AS UNID,
                FORMAT(CASE WHEN a.dCantidad = 0 THEN 0 ELSE (COALESCE(programado.PROG, 0) / a.dCantidad) END, '0%') AS PROGP,
                FORMAT(CASE WHEN a.dCantidad = 0 THEN 0 ELSE (COALESCE(cortado.CORTADO, 0) / a.dCantidad) END, '0%') AS CORTADOP,
                FORMAT(CASE WHEN a.dCantidad = 0 THEN 0 ELSE (COALESCE(cosido.COSIDO, 0) / a.dCantidad) END, '0%') AS COSIDOP
            FROM docOrdenVenta a
            INNER JOIN maeAnexoCliente b ON a.IdmaeAnexo_Cliente = b.IdmaeAnexo_Cliente
            LEFT JOIN (
                SELECT
                    c.IdDocumento_Referencia AS PEDIDO,
                    SUM(c.dCantidad) AS KG
                FROM docOrdenVentaItem c
                WHERE c.IdDocumento_Referencia > 0
                GROUP BY c.IdDocumento_Referencia
            ) d ON a.IdDocumento_OrdenVenta = d.PEDIDO
            LEFT JOIN (
                SELECT
                    x.IdDocumento_Referencia AS PEDIDO,
                    SUM(y.dCantidadProgramado) AS KG_ARM,
                    SUM(z.bcerrado * y.dCantidadRequerido) AS KG_PRODUC,
                    SUM(s.bcerrado * y.dCantidadProgramado) AS KG_TEÑIDOS
                FROM docOrdenProduccionItem y
                INNER JOIN docOrdenProduccion z ON y.IdDocumento_OrdenProduccion = z.IdDocumento_OrdenProduccion
                INNER JOIN docOrdenVentaItem x ON (z.IdDocumento_Referencia = x.IdDocumento_OrdenVenta AND y.idmaeItem = x.IdmaeItem)
                INNER JOIN docOrdenProduccionRuta s ON y.IdDocumento_OrdenProduccion = s.IdDocumento_OrdenProduccion
                WHERE s.IdmaeReceta > 0
                GROUP BY x.IdDocumento_Referencia
            ) t ON a.IdDocumento_OrdenVenta = t.PEDIDO
            LEFT JOIN (
                SELECT 
                    g.IdDocumento_OrdenVenta,
                    SUM(a.dCantidadProgramado) AS PROG
                FROM dbo.docOrdenProduccion c WITH (NOLOCK)
                INNER JOIN dbo.docOrdenProduccionItem a WITH (NOLOCK)
                    ON c.IdDocumento_OrdenProduccion = a.IdDocumento_OrdenProduccion
                INNER JOIN dbo.docOrdenVenta g WITH (NOLOCK)
                    ON c.IdDocumento_Referencia = g.IdDocumento_OrdenVenta
                INNER JOIN dbo.docOrdenProduccionRuta b WITH (NOLOCK)
                    ON c.IdDocumento_OrdenProduccion = b.IdDocumento_OrdenProduccion
                WHERE c.bAnulado = 0
                    AND c.IdtdDocumentoForm = 127
                    AND b.IdmaeCentroCosto = 29
                GROUP BY g.IdDocumento_OrdenVenta
            ) AS programado
            ON a.IdDocumento_OrdenVenta = programado.IdDocumento_OrdenVenta
            LEFT JOIN (
                SELECT 
                    g.IdDocumento_OrdenVenta,
                    SUM(b.dCantidadIng) AS CORTADO
                FROM dbo.docNotaInventario a WITH (NOLOCK)
                INNER JOIN dbo.maeCentroCosto a1 WITH (NOLOCK)
                    ON a.IdmaeCentroCosto = a1.IdmaeCentroCosto
                    AND a1.bConOrdenProduccion = 1
                INNER JOIN dbo.docNotaInventarioItem b WITH (NOLOCK)
                    ON a.IdDocumento_NotaInventario = b.IdDocumento_NotaInventario
                INNER JOIN dbo.docOrdenProduccion c WITH (NOLOCK)
                    ON a.IdDocumento_OrdenProduccion = c.IdDocumento_OrdenProduccion
                INNER JOIN dbo.docOrdenVenta g WITH (NOLOCK)
                    ON c.IdDocumento_Referencia = g.IdDocumento_OrdenVenta
                WHERE a.IdtdDocumentoForm = 131
                    AND a.bDevolucion = 0
                    AND a.bDesactivado = 0
                    AND a.bAnulado = 0
                    AND a.IdmaeCentroCosto = 29
                GROUP BY g.IdDocumento_OrdenVenta
            ) AS cortado
            ON a.IdDocumento_OrdenVenta = cortado.IdDocumento_OrdenVenta
            LEFT JOIN (
                SELECT 
                    g.IdDocumento_OrdenVenta,
                    SUM(b.dCantidadIng) AS COSIDO
                FROM dbo.docNotaInventario a WITH (NOLOCK)
                INNER JOIN dbo.maeCentroCosto a1 WITH (NOLOCK)
                    ON a.IdmaeCentroCosto = a1.IdmaeCentroCosto
                    AND a1.bConOrdenProduccion = 1
                INNER JOIN dbo.docNotaInventarioItem b WITH (NOLOCK)
                    ON a.IdDocumento_NotaInventario = b.IdDocumento_NotaInventario
                INNER JOIN dbo.docOrdenProduccion c WITH (NOLOCK)
                    ON a.IdDocumento_OrdenProduccion = c.IdDocumento_OrdenProduccion
                INNER JOIN dbo.docOrdenVenta g WITH (NOLOCK)
                    ON c.IdDocumento_Referencia = g.IdDocumento_OrdenVenta
                WHERE a.IdtdDocumentoForm = 131
                    AND a.bDevolucion = 0
                    AND a.bDesactivado = 0
                    AND a.bAnulado = 0
                    AND a.IdmaeCentroCosto = 47
                GROUP BY g.IdDocumento_OrdenVenta
            ) AS cosido
            ON a.IdDocumento_OrdenVenta = cosido.IdDocumento_OrdenVenta
            WHERE
                a.IdtdDocumentoForm = 10
                AND a.IdtdTipoVenta = 4
                AND a.bAnulado = 0
            ) gg
        INNER JOIN 
            (SELECT 
                x.IdDocumento_OrdenVenta,
                q0.FMINARM,
                q0.FMAXARM,
                q1.FMINTENID,
                q1.FMAXTENID,
                q2.FMINTELAPROB,
                q2.FMAXTELAPROB,
                q3.FMINCORTE,
                q3.FMAXCORTE,
                q4.FMINCOSIDO,
                q4.FMAXCOSIDO
            FROM docOrdenVenta x
            LEFT JOIN (
                SELECT 
                    x.IdDocumento_OrdenVenta, 
                    MIN(b.dtFechaEmision) AS FMINARM,
                    MAX(b.dtFechaEmision) AS FMAXARM
                FROM docOrdenVentaItem a
                INNER JOIN docOrdenProduccion b ON b.IdDocumento_Referencia = a.IdDocumento_OrdenVenta
                INNER JOIN docOrdenVenta x ON a.IdDocumento_Referencia = x.IdDocumento_OrdenVenta
                WHERE b.IdtdDocumentoForm = 138 
                    AND b.IdtdDocumentoForm_Referencia = 152 
                    AND x.CoddocOrdenVenta IS NOT NULL
                    AND a.IdDocumento_Referencia > 0
                GROUP BY x.IdDocumento_OrdenVenta
            ) q0 ON x.IdDocumento_OrdenVenta = q0.IdDocumento_OrdenVenta
            LEFT JOIN (
                SELECT 
                    x.IdDocumento_OrdenVenta, 
                    MIN(e.dtFechaHoraFin) AS FMINTENID,
                    MAX(e.dtFechaHoraFin) AS FMAXTENID
                FROM docOrdenVentaItem a
                INNER JOIN docOrdenProduccion b ON b.IdDocumento_Referencia = a.IdDocumento_OrdenVenta
                INNER JOIN docOrdenVenta x ON a.IdDocumento_Referencia = x.IdDocumento_OrdenVenta
                INNER JOIN docRecetaOrdenProduccion d ON b.IdDocumento_OrdenProduccion = d.IdDocumento_OrdenProduccion
                INNER JOIN docReceta e ON d.IdDocumento_Receta = e.IdDocumento_Receta
                WHERE b.IdtdDocumentoForm = 138 
                    AND b.IdtdDocumentoForm_Referencia = 152 
                    AND x.CoddocOrdenVenta IS NOT NULL
                    AND a.IdDocumento_Referencia > 0
                GROUP BY x.IdDocumento_OrdenVenta
            ) q1 ON x.IdDocumento_OrdenVenta = q1.IdDocumento_OrdenVenta
            LEFT JOIN (
                SELECT 
                    x.IdDocumento_OrdenVenta,  
                    MIN(b.FechaCierreAprobado) AS FMINTELAPROB,
                    MAX(b.FechaCierreAprobado) AS FMAXTELAPROB
                FROM docOrdenVentaItem a
                INNER JOIN docOrdenProduccion b ON b.IdDocumento_Referencia = a.IdDocumento_OrdenVenta
                INNER JOIN docOrdenVenta x ON a.IdDocumento_Referencia = x.IdDocumento_OrdenVenta
                INNER JOIN docOrdenProduccionRuta d ON b.IdDocumento_OrdenProduccion = d.IdDocumento_OrdenProduccion
                WHERE b.IdtdDocumentoForm = 138 
                    AND b.IdtdDocumentoForm_Referencia = 152 
                    AND x.CoddocOrdenVenta IS NOT NULL
                    AND a.IdDocumento_Referencia > 0
                GROUP BY x.IdDocumento_OrdenVenta
            ) q2 ON x.IdDocumento_OrdenVenta = q2.IdDocumento_OrdenVenta
            LEFT JOIN (
                SELECT 
                    g.IdDocumento_OrdenVenta,  
                    MIN(a.dtFechaRegistro) AS FMINCORTE,
                    MAX(a.dtFechaRegistro) AS FMAXCORTE
                FROM dbo.docNotaInventario a WITH (NOLOCK)
                INNER JOIN dbo.maeCentroCosto a1 WITH (NOLOCK) ON a.IdmaeCentroCosto = a1.IdmaeCentroCosto AND a1.bConOrdenProduccion = 1
                INNER JOIN dbo.docNotaInventarioItem b WITH (NOLOCK) ON a.IdDocumento_NotaInventario = b.IdDocumento_NotaInventario AND b.dCantidadIng <> 0
                INNER JOIN dbo.docOrdenProduccion c WITH (NOLOCK) ON a.IdDocumento_OrdenProduccion = c.IdDocumento_OrdenProduccion 
                AND c.bAnulado = 0 AND c.IdtdDocumentoForm = 127
                INNER JOIN dbo.docOrdenVenta g WITH (NOLOCK) ON c.IdDocumento_Referencia = g.IdDocumento_OrdenVenta
                INNER JOIN dbo.docOrdenProduccionRuta d WITH (NOLOCK) ON a.IddocOrdenProduccionRuta = d.IddocOrdenProduccionRuta
                INNER JOIN dbo.docOrdenProduccionItem e WITH (NOLOCK) ON c.IdDocumento_OrdenProduccion = e.IdDocumento_OrdenProduccion AND b.IdmaeItem_Inventario = e.IdmaeItem
                INNER JOIN dbo.maeItemInventario f WITH (NOLOCK) ON b.IdmaeItem_Inventario = f.IdmaeItem_Inventario AND f.IdtdItemForm = 10
                WHERE a.IdtdDocumentoForm = 131
                    AND a.bDevolucion = 0
                    AND a.bDesactivado = 0
                    AND a.bAnulado = 0
                    AND a.IdDocumento_OrdenProduccion <> 0
                    AND a.IdmaeCentroCosto = 29
                GROUP BY g.IdDocumento_OrdenVenta
            ) q3 ON x.IdDocumento_OrdenVenta = q3.IdDocumento_OrdenVenta
            LEFT JOIN (
                SELECT 
                    g.IdDocumento_OrdenVenta,  
                    MIN(a.dtFechaRegistro) AS FMINCOSIDO,
                    MAX(a.dtFechaRegistro) AS FMAXCOSIDO
                FROM dbo.docNotaInventario a WITH (NOLOCK)
                INNER JOIN dbo.maeCentroCosto a1 WITH (NOLOCK) ON a.IdmaeCentroCosto = a1.IdmaeCentroCosto AND a1.bConOrdenProduccion = 1
                INNER JOIN dbo.docNotaInventarioItem b WITH (NOLOCK) ON a.IdDocumento_NotaInventario = b.IdDocumento_NotaInventario AND b.dCantidadIng <> 0
                INNER JOIN dbo.docOrdenProduccion c WITH (NOLOCK) ON a.IdDocumento_OrdenProduccion = c.IdDocumento_OrdenProduccion 
                AND c.bAnulado = 0 AND c.IdtdDocumentoForm = 127
                INNER JOIN dbo.docOrdenVenta g WITH (NOLOCK) ON c.IdDocumento_Referencia = g.IdDocumento_OrdenVenta
                INNER JOIN dbo.docOrdenProduccionRuta d WITH (NOLOCK) ON a.IddocOrdenProduccionRuta = d.IddocOrdenProduccionRuta
                INNER JOIN dbo.docOrdenProduccionItem e WITH (NOLOCK) ON c.IdDocumento_OrdenProduccion = e.IdDocumento_OrdenProduccion AND b.IdmaeItem_Inventario = e.IdmaeItem
                INNER JOIN dbo.maeItemInventario f WITH (NOLOCK) ON b.IdmaeItem_Inventario = f.IdmaeItem_Inventario AND f.IdtdItemForm = 10
                WHERE a.IdtdDocumentoForm = 131
                    AND a.bDevolucion = 0
                    AND a.bDesactivado = 0
                    AND a.bAnulado = 0
                    AND a.IdDocumento_OrdenProduccion <> 0
                    AND a.IdmaeCentroCosto = 47
                GROUP BY g.IdDocumento_OrdenVenta
            ) q4 ON x.IdDocumento_OrdenVenta = q4.IdDocumento_OrdenVenta
            WHERE x.CoddocOrdenVenta IS NOT NULL
                AND x.IdtdDocumentoForm = 10 
                AND x.IdtdTipoVenta = 4
                AND x.bAnulado = 0
            ) ff
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
                # Después de st.dataframe(df_postgres), agrega este código:

                # Crear tabla de avance de procesos
                st.subheader("Avance de Procesos por Pedido")

                # Obtener la fecha actual
                current_date = datetime.now().date()

                # Preparar el dataframe para el análisis de avance
                def prepare_process_progress(df_mssql, df_postgres):
                    """Prepara un dataframe con el avance de los procesos."""
                    # Crear un dataframe base con los pedidos
                    if df_mssql.empty or df_postgres.empty:
                        return pd.DataFrame()
                    
                    # Unir los dataframes por pedido
                    processes_df = pd.DataFrame()
                    
                    for _, row in df_mssql.iterrows():
                        if row['PEDIDO'] == 'RESUMEN':
                            continue
                            
                        pedido = row['PEDIDO']
                        postgres_row = df_postgres[df_postgres['pedido'] == pedido]
                        
                        if postgres_row.empty:
                            continue
                        
                        # Para cada proceso, calcular el avance
                        processes = [
                            {
                                'pedido': pedido,
                                'proceso': 'Armado',
                                'inicio': postgres_row['star_armado'].values[0] if not postgres_row['star_armado'].isna().all() else None,
                                'fin': postgres_row['finish_armado'].values[0] if not postgres_row['finish_armado'].isna().all() else None,
                                'porcentaje': row['KG_ARMP']
                            },
                            {
                                'pedido': pedido,
                                'proceso': 'Teñido',
                                'inicio': postgres_row['star_tenido'].values[0] if not postgres_row['star_tenido'].isna().all() else None,
                                'fin': postgres_row['finish_tenido'].values[0] if not postgres_row['finish_tenido'].isna().all() else None,
                                'porcentaje': row['KG_TENIDP']
                            },
                            {
                                'pedido': pedido,
                                'proceso': 'Tela Aprobada',
                                'inicio': postgres_row['star_telaprob'].values[0] if not postgres_row['star_telaprob'].isna().all() else None,
                                'fin': postgres_row['finish_telaprob'].values[0] if not postgres_row['finish_telaprob'].isna().all() else None,
                                'porcentaje': row['KG_TELAPROBP']
                            },
                            {
                                'pedido': pedido,
                                'proceso': 'Corte',
                                'inicio': postgres_row['star_corte'].values[0] if not postgres_row['star_corte'].isna().all() else None,
                                'fin': postgres_row['finish_corte'].values[0] if not postgres_row['finish_corte'].isna().all() else None,
                                'porcentaje': row['CORTADOP']
                            },
                            {
                                'pedido': pedido,
                                'proceso': 'Costura',
                                'inicio': postgres_row['star_costura'].values[0] if not postgres_row['star_costura'].isna().all() else None,
                                'fin': postgres_row['finish_costura'].values[0] if not postgres_row['finish_costura'].isna().all() else None,
                                'porcentaje': row['COSIDOP']
                            }
                        ]
                        
                        temp_df = pd.DataFrame(processes)
                        processes_df = pd.concat([processes_df, temp_df], ignore_index=True)
                    
                    return processes_df

                # Calcular las métricas adicionales
                def calculate_process_metrics(processes_df, current_date):
                    """Calcula las métricas de avance para cada proceso."""
                    if processes_df.empty:
                        return pd.DataFrame()
                    
                    # Convertir porcentajes a números
                    processes_df['porcentaje_num'] = processes_df['porcentaje'].apply(
                        lambda x: float(str(x).rstrip('%')) if pd.notna(x) and isinstance(x, str) else 0
                    )
                    
                    # Calcular días hasta finalización
                    def calc_days_to_finish(end_date):
                        if pd.isna(end_date):
                            return None
                        if isinstance(end_date, str):
                            try:
                                end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                            except ValueError:
                                return None
                        return (end_date - current_date).days
                    
                    processes_df['dias_para_finalizar'] = processes_df['fin'].apply(calc_days_to_finish)
                    
                    # Calcular el porcentaje de tiempo transcurrido
                    def calc_time_percentage(row):
                        if pd.isna(row['inicio']) or pd.isna(row['fin']):
                            return None
                            
                        try:
                            if isinstance(row['inicio'], str):
                                start_date = datetime.strptime(row['inicio'], '%Y-%m-%d').date()
                            else:
                                start_date = row['inicio']
                                
                            if isinstance(row['fin'], str):
                                end_date = datetime.strptime(row['fin'], '%Y-%m-%d').date()
                            else:
                                end_date = row['fin']
                                
                            total_days = (end_date - start_date).days
                            elapsed_days = (current_date - start_date).days
                            
                            if total_days <= 0:
                                return 100.0
                                
                            percentage = min(100.0, max(0.0, (elapsed_days / total_days) * 100))
                            return round(percentage, 1)
                        except Exception:
                            return None
                    
                    processes_df['porcentaje_tiempo'] = processes_df.apply(calc_time_percentage, axis=1)
                    
                    # Formatear porcentajes para visualización
                    processes_df['porcentaje_tiempo_fmt'] = processes_df['porcentaje_tiempo'].apply(
                        lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A"
                    )
                    
                    return processes_df

                # Ejecutar el análisis
                processes_df = prepare_process_progress(df, df_postgres)
                if not processes_df.empty:
                    processes_with_metrics = calculate_process_metrics(processes_df, current_date)
                    
                    # Preparar dataframe para visualización
                    display_df = processes_with_metrics[['pedido', 'proceso', 'porcentaje', 'dias_para_finalizar', 'porcentaje_tiempo_fmt']]
                    display_df.columns = ['Pedido', 'Proceso', 'Avance Real', 'Días para Finalizar', 'Avance Tiempo (%)']
                    
                    # Mostrar tabla
                    st.dataframe(display_df)
                    
                    # Crear gráfico comparativo de avance real vs tiempo
                    st.subheader("Comparativa de Avance Real vs Tiempo Transcurrido")
                    
                    for pedido in processes_with_metrics['pedido'].unique():
                        pedido_df = processes_with_metrics[processes_with_metrics['pedido'] == pedido]
                        
                        fig = go.Figure()
                        
                        # Añadir barras para el avance real
                        fig.add_trace(go.Bar(
                            x=pedido_df['proceso'],
                            y=pedido_df['porcentaje_num'],
                            name='Avance Real (%)',
                            marker_color='royalblue'
                        ))
                        
                        # Añadir barras para el avance de tiempo
                        fig.add_trace(go.Bar(
                            x=pedido_df['proceso'],
                            y=pedido_df['porcentaje_tiempo'],
                            name='Tiempo Transcurrido (%)',
                            marker_color='orange'
                        ))
                        
                        # Configurar el diseño
                        fig.update_layout(
                            title=f'Pedido: {pedido}',
                            xaxis_title='Proceso',
                            yaxis_title='Porcentaje (%)',
                            barmode='group',
                            yaxis=dict(range=[0, 105]),
                            height=400,
                            width=800
                        )
                        
                        st.plotly_chart(fig)
                        
                        # Línea divisoria entre pedidos
                        st.markdown("---")
                else:
                    st.warning("No hay datos disponibles para calcular el avance de procesos.")
                                
        except Exception as e:
            st.error(f"Error al ejecutar la consulta: {e}")
    else:
        st.warning("Por favor ingresa un número de pedido.")
