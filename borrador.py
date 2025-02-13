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
        'Start Real': [pd.to_datetime(df['FMINARM'].iloc[n]), pd.to_datetime(df['FMINTENID'].iloc[n]), 
                       pd.to_datetime(df['FMINTELAPROB'].iloc[n]), pd.to_datetime(df['FMINCORTE'].iloc[n]), 
                       pd.to_datetime(df['FMINCOSIDO'].iloc[n])],
        'Finish Real': [pd.to_datetime(df['FMAXARM'].iloc[n]), pd.to_datetime(df['FMAXTENID'].iloc[n]), 
                        pd.to_datetime(df['FMAXTELAPROB'].iloc[n]), pd.to_datetime(df['FMAXCORTE'].iloc[n]), 
                        pd.to_datetime(df['FMAXCOSIDO'].iloc[n])],
        'Avance': [df['KG_ARMP'].iloc[n], df['KG_TENIDP'].iloc[n], df['KG_TELAPROBP'].iloc[n], 
                   df['CORTADOP'].iloc[n], df['COSIDOP'].iloc[n]]
    })
    
    # Crear el gráfico de Gantt
    fig = px.timeline(df_gantt, x_start="Start", x_end="Finish", y="Proceso", text="Avance")
    
    # Agregar líneas verticales cada dos días
    fecha_inicio = min(df_gantt['Start'].min(), df_gantt['Start Real'].min())
    fecha_fin = max(df_gantt['Finish'].max(), df_gantt['Finish Real'].max())
    dias_totales = (fecha_fin - fecha_inicio).days

    for i in range(0, dias_totales + 1, 2):
        fecha_linea = fecha_inicio + timedelta(days=i)
        fig.add_shape(
            type="line",
            x0=fecha_linea,
            y0=0,
            x1=fecha_linea,
            y1=len(df_gantt),
            line=dict(
                color="lightgray",
                width=1,
                dash="dot"
            ),
            layer="below"  # Esto asegura que las líneas estén detrás de las barras del Gantt
        )

    # Agregar las marcas de inicio y fin reales
    fig.add_trace(go.Scatter(
        x=df_gantt['Start Real'],
        y=df_gantt['Proceso'],
        mode='markers',
        marker=dict(symbol='triangle-up', size=10, color='black'),
        name='Inicio Real'
    ))
    fig.add_trace(go.Scatter(
        x=df_gantt['Finish Real'],
        y=df_gantt['Proceso'],
        mode='markers',
        marker=dict(symbol='triangle-down', size=10, color='red'),
        name='Fin Real'
    ))

    # Fechas de emisión y entrega
    fecha_emision = pd.to_datetime(df['F_EMISION'].iloc[n])
    fecha_entrega = pd.to_datetime(df['F_ENTREGA'].iloc[n])

    # Fechas de inicio y fin del pedido
    fecha_inicio_pedido = min(inicial,df_gantt['Start'].min(), df_gantt['Start Real'].min())
    fecha_fin_pedido = max(fin,df_gantt['Finish'].max(), df_gantt['Finish Real'].max())

    # Agregar líneas verticales para las fechas de emisión y entrega
    fig.add_shape(
        type="line",
        x0=fecha_emision,
        y0=0,
        x1=fecha_emision,
        y1=len(df_gantt),
        line=dict(color="green", width=2, dash="dash"),
        name="Fecha Emisión"
    )
    fig.add_annotation(
        x=fecha_emision,
        y=len(df_gantt)/2,
        text="Emisión<br>" + fecha_emision.strftime('%b %d'),
        showarrow=True,
        arrowhead=1
    )
    fig.add_shape(
        type="line",
        x0=fecha_entrega,
        y0=0,
        x1=fecha_entrega,
        y1=len(df_gantt),
        line=dict(color="red", width=2, dash="dash"),
        name="Fecha Entrega"
    )
    fig.add_annotation(
        x=fecha_entrega,
        y=len(df_gantt)/2,
        text="Entrega<br>" + fecha_entrega.strftime('%b %d'),
        showarrow=True,
        arrowhead=1
    )

    # Agregar líneas verticales para las fechas de inicio y fin del pedido
    fig.add_shape(
        type="line",
        x0=fecha_inicio_pedido,
        y0=0,
        x1=fecha_inicio_pedido,
        y1=len(df_gantt),
        line=dict(color="purple", width=2, dash="dash"),
        name="Inicio Pedido"
    )
    fig.add_annotation(
        x=fecha_inicio_pedido,
        y=len(df_gantt)/2,
        text="Inicio<br>" + fecha_inicio_pedido.strftime('%b %d'),
        showarrow=True,
        arrowhead=1
    )
    fig.add_shape(
        type="line",
        x0=fecha_fin_pedido,
        y0=0,
        x1=fecha_fin_pedido,
        y1=len(df_gantt),
        line=dict(color="orange", width=2, dash="dash"),
        name="Fin Pedido"
    )
    fig.add_annotation(
        x=fecha_fin_pedido,
        y=len(df_gantt)/2,
        text="Fin<br>" + fecha_fin_pedido.strftime('%b %d'),
        showarrow=True,
        arrowhead=1
    )

    # Agregar una línea vertical para la fecha actual
    fecha_actual = datetime.now().strftime('%Y-%m-%d')
    fig.add_shape(
        type="line",
        x0=fecha_actual,
        y0=0,
        x1=fecha_actual,
        y1=len(df_gantt),
        line=dict(color="blue", width=2, dash="dash"),
        name="Fecha Actual"
    )

    # Ajustar el diseño del gráfico
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
