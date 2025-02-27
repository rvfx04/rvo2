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

# Función para convertir columnas de fecha a solo fecha (sin hora)
def convert_date_columns(df):
    """Convierte todas las columnas de fecha a solo fecha (sin hora)."""
    date_columns = [col for col in df.columns if any(x in col.upper() for x in ['FECHA', 'FMIN', 'FMAX', 'F_EMISION', 'F_ENTREGA'])]
    
    for col in date_columns:
        if col in df.columns:
            # Convertir a datetime primero (si no lo es ya)
            if df[col].dtype != 'datetime64[ns]':
                df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Extraer solo la fecha
            df[col] = df[col].dt.date
    
    return df

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
    
    # Leer los datos
    df = pd.read_sql(query, conn, params=tuple(pedidos))
    
    # Convertir todas las columnas de fecha a solo fecha
    df = convert_date_columns(df)
    
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
                
                # Mostrar tabla adicional con el avance de cada proceso
                st.subheader("Avance de Procesos por Pedido")

                # Modificar la parte problemática del código (líneas 402-433 aproximadamente)
                # Modificar la parte problemática del código evitando el uso de isinstance()
                procesos = ['armado', 'tenido', 'telaprob', 'corte', 'costura']
                avance_data = []

                

                # Crear un diccionario para buscar el pedido correspondiente en el DataFrame df
                # Esto permite relacionar los datos entre ambas tablas
                df_dict = {row['PEDIDO']: row for _, row in df.iterrows() if 'PEDIDO' in df.columns}

                for _, row in df_postgres.iterrows():
                    pedido = row['pedido']
                    for proceso in procesos:
                        # Las fechas ya deben ser objetos date a este punto
                        fecha_inicio = row[f'star_{proceso}']
                        fecha_fin = row[f'finish_{proceso}']
                        
                        # Verificar que las fechas no sean nulas
                        if pd.notna(fecha_inicio) and pd.notna(fecha_fin):
                            try:
                                # Intentar convertir a datetime primero
                                if hasattr(fecha_inicio, 'date'):
                                    # Es un objeto timestamp o datetime
                                    fecha_inicio = fecha_inicio.date()
                                else:
                                    # Es otro tipo de objeto, intentar convertir
                                    fecha_inicio = pd.to_datetime(fecha_inicio).date()
                                    
                                if hasattr(fecha_fin, 'date'):
                                    fecha_fin = fecha_fin.date()
                                else:
                                    fecha_fin = pd.to_datetime(fecha_fin).date()
                                
                                fecha_actual = datetime.now().date()  # Obtener solo la fecha actual
                                
                                diferencia_dias = (fecha_fin - fecha_actual).days
                                
                                # Evitar división por cero
                                #if (fecha_fin - fecha_inicio).days > 0:
                                porcentaje_avance = ((fecha_actual - fecha_inicio).days / (fecha_fin - fecha_inicio).days) * 100
                                #    # Limitar el porcentaje entre 0 y 100
                                #    porcentaje_avance = max(0, min(100, porcentaje_avance))
                                #else:
                                #    porcentaje_avance = 100 if fecha_actual >= fecha_fin else 0
                                
                                # Agregar el valor de avance de la primera tabla según el tipo de proceso
                                avance_valor = ""
                                if pedido in df_dict:
                                    if proceso == 'armado':
                                        avance_valor = df_dict[pedido]['KG_ARMP']
                                    elif proceso == 'tenido':
                                        avance_valor = df_dict[pedido]['KG_TENIDP']
                                    elif proceso == 'telaprob':
                                        avance_valor = df_dict[pedido]['KG_TELAPROBP']
                                    elif proceso == 'corte':
                                        avance_valor = df_dict[pedido]['CORTADOP']
                                    elif proceso == 'costura':
                                        avance_valor = df_dict[pedido]['COSIDOP']
                                
                                avance_data.append({
                                    'PEDIDO': pedido,
                                    'PROCESO': proceso,
                                    'RETRASO_DIAS': diferencia_dias,
                                    'AVANCE_HOY': f"{porcentaje_avance:.2f}%",
                                    'AVANCE': avance_valor  # Nueva columna
                                })
                            except Exception as e:
                                # En caso de error, mostrar el proceso que falló pero continuar con los demás
                                st.warning(f"Error al procesar fecha para {pedido}, proceso {proceso}: {e}")
                                continue

                            # Crear el DataFrame de avance si hay datos
                            if avance_data:
                                df_avance = pd.DataFrame(avance_data)
                                st.dataframe(df_avance)
                            else:
                                st.warning("No hay datos de avance disponibles.")
                                                                            
        except Exception as e:
            st.error(f"Error al ejecutar la consulta: {e}")
            st.exception(e)  # Esto muestra el traceback completo para depuración
    else:
        st.warning("Por favor ingresa un número de pedido.")
