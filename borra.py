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

# New PostgreSQL connection function
def connect_postgres():
    connection = psycopg2.connect(
        host=st.secrets["host"],
        port=st.secrets["port"],
        database=st.secrets["database"],
        user=st.secrets["user"],
        password=st.secrets["password"]
    )
    return connection

# Agregar estas funciones después de connect_postgres():

def add_summary_row_sql(df):
    # Create a summary row
    summary = pd.Series({
        'PEDIDO': 'RESUMEN',
        'F_EMISION': df['F_EMISION'].min(),
        'F_ENTREGA': df['F_ENTREGA'].min(),
        'DIAS': None,  # Not applicable for summary
        'CLIENTE': 'TOTAL',
        'PO': 'TOTAL',
        'KG_REQ': df['KG_REQ'].sum(),
        'UNID': df['UNID'].sum(),
    })
    
    # Handle percentage columns with weighted averages
    def calculate_weighted_percentage(row):
        kg_req = float(row['KG_REQ'])
        return kg_req if not pd.isna(kg_req) else 0
    
    total_kg = df['KG_REQ'].sum()
    
    # Calculate weighted percentages for KG related columns
    for col in ['KG_ARMP', 'KG_TENIDP', 'KG_TELAPROBP']:
        weighted_sum = sum(
            float(str(row[col]).rstrip('%')) * calculate_weighted_percentage(row)
            for _, row in df.iterrows()
        )
        summary[col] = f"{(weighted_sum / total_kg if total_kg > 0 else 0):.0f}%"
    
    # Calculate weighted percentages for unit related columns
    total_units = df['UNID'].sum()
    for col in ['PROGP', 'CORTADOP', 'COSIDOP']:
        weighted_sum = sum(
            float(str(row[col]).rstrip('%')) * row['UNID']
            for _, row in df.iterrows()
        )
        summary[col] = f"{(weighted_sum / total_units if total_units > 0 else 0):.0f}%"
    
    # Handle date columns
    min_date_cols = ['FMINARM', 'FMINTENID', 'FMINTELAPROB', 'FMINCORTE', 'FMINCOSIDO']
    max_date_cols = ['FMAXARM', 'FMAXTENID', 'FMAXTELAPROB', 'FMAXCORTE', 'FMAXCOSIDO']
    
    for col in min_date_cols:
        summary[col] = df[col].min()
    
    for col in max_date_cols:
        summary[col] = df[col].max()
    
    # Append summary row to dataframe
    df_with_summary = pd.concat([df, pd.DataFrame([summary])], ignore_index=True)
    return df_with_summary

def add_summary_row_postgres(df):
    # Create a summary row
    summary = pd.Series({
        'pedido': 'RESUMEN',
        'Fecha_Colocacion': df['Fecha_Colocacion'].min(),
        'Fecha_Entrega': df['Fecha_Entrega'].min(),
    })
    
    # Handle start dates (minimum)
    start_cols = ['star_armado', 'star_tenido', 'star_telaprob', 'star_corte', 'star_costura']
    for col in start_cols:
        summary[col] = df[col].min()
    
    # Handle finish dates (maximum)
    finish_cols = ['finish_armado', 'finish_tenido', 'finish_telaprob', 'finish_corte', 'finish_costura']
    for col in finish_cols:
        summary[col] = df[col].max()
    
    # Append summary row to dataframe
    df_with_summary = pd.concat([df, pd.DataFrame([summary])], ignore_index=True)
    return df_with_summary

# Función para ejecutar la consulta SQL
def run_query(pedidos):
    conn = connect_db()
    query = """SELECT gg.PEDIDO, --gg.IdDocumento_OrdenVenta, 
    	gg.F_EMISION, gg.F_ENTREGA, gg.DIAS, gg.CLIENTE, gg.PO, gg.KG_REQ, 
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
    WHERE --c.bCerrado = 0  AND
        c.bAnulado = 0
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
    --AND (CASE WHEN ISDATE(a.dtFechaEntrega) = --1 THEN CONVERT(DATE, a.dtFechaEntrega) ELSE --NULL END) BETWEEN '2024-08-01' AND --'2024-12-31' 
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
		and x.IdtdDocumentoForm=10 
		and x.IdtdTipoVenta=4
		and x.bAnulado=0
    ) ff
ON gg.IdDocumento_OrdenVenta = ff.IdDocumento_OrdenVenta
WHERE gg.PEDIDO IN ({})""".format(','.join(['?' for _ in pedidos])) 

    df = pd.read_sql(query, conn, params=tuple(pedidos))
    conn.close()
    return df

# New PostgreSQL query function
def run_postgres_query(pedido):
    conn = connect_postgres()
    placeholders = ','.join(['%s' for _ in pedidos])
    # Modify this query to get the specific dates and information you want
    query = '''
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
    '''.format(placeholders)
    
    df = pd.read_sql(query, conn, params=tuple(pedidos))
    conn.close()
    return df




# Interfaz de usuario
st.title("Progreso de Pedidos Consolidado")

pedidos_input = st.text_input("Ingresa los números de pedido (separados por coma)")

if st.button("Ejecutar Consulta"):
    if pedidos_input:
        try:
            pedidos = [p.strip() for p in pedidos_input.split(',')]
            
            # Ejecutar consultas
            df = run_query(pedidos)
            df_postgres = run_postgres_query(pedidos)
            
            if df.empty:
                st.warning("No se encontraron datos para estos pedidos en SQL Server.")
            else:
                # Add summary rows to both dataframes
                df = add_summary_row_sql(df)
                df_postgres = add_summary_row_postgres(df_postgres)
                
                # Mostrar resumen de datos
                st.subheader("Resumen de Pedidos")
                summary_df = pd.DataFrame({
                    'Total Unidades': [df['UNID'].sum()],
                    'Total KG': [df['KG_REQ'].sum()],
                    'Promedio Días': [df['DIAS'].mean()],
                    'Cantidad Pedidos': [len(df)-1]
                })
                st.dataframe(summary_df)
                n = len(df)-1
                # Mostrar datos detallados con resumen
                st.subheader("Detalle por Pedido (SQL Server)")
                st.dataframe(df)
                
                st.subheader("Detalle por Pedido (PostgreSQL)")
                st.dataframe(df_postgres)
                
                # Procesar los datos para el gráfico de Gantt
                f_emision = pd.to_datetime(df['F_EMISION'].iloc[n])
                #dias = df['DIAS'].iloc[0]

                # Cálculo de las fechas de inicio y fin
                
                start_armado = pd.to_datetime(df_postgres['star_armado'].iloc[n]) 
		  
                start_tenido = pd.to_datetime(df_postgres['star_tenido'].iloc[n])
                start_telaprob = pd.to_datetime(df_postgres['star_telaprob'].iloc[n])
                start_corte = pd.to_datetime(df_postgres['star_corte'].iloc[n])
                start_costura = pd.to_datetime(df_postgres['star_costura'].iloc[n])
                finish_armado = pd.to_datetime(df_postgres['finish_armado'].iloc[n])
                finish_tenido = pd.to_datetime(df_postgres['finish_tenido'].iloc[n])
                finish_telaprob = pd.to_datetime(df_postgres['finish_telaprob'].iloc[n])
                finish_corte = pd.to_datetime(df_postgres['finish_corte'].iloc[n])
                finish_costura = pd.to_datetime(df_postgres['finish_costura'].iloc[n])
                inicial = pd.to_datetime(df_postgres['Fecha_Colocacion'].iloc[n])
                fin = pd.to_datetime(df_postgres['Fecha_Entrega'].iloc[n]) 
		

                 
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

	        # Cambiar el color de las barras
                for trace in fig.data:
                    trace.marker.color = 'lightsteelblue'  # Puedes cambiar a cualquier color válido

                # Mostrar las etiquetas del eje X cada 7 días
                tick0_date = f_emision.strftime('%Y-%m-%d')
                fig.update_xaxes(tickmode='linear', tick0=tick0_date, dtick=7 * 24 * 60 * 60 * 1000)

                # Ajustar el diseño del gráfico
                fig.update_yaxes(autorange="reversed")

                # Agregar las barras de las fechas reales
                fig.add_trace(go.Scatter(
                    x=df_gantt['Start Real'],
                    y=df_gantt['Proceso'],
                    mode='markers',
                    #marker=dict(color='black', size=10),
		    marker=dict(symbol='triangle-up', size=10, color='black'),	
                    name='Start Real'
                ))
                fig.add_trace(go.Scatter(
                    x=df_gantt['Finish Real'],
                    y=df_gantt['Proceso'],
                    mode='markers',
		    marker=dict(symbol='triangle-down', size=10, color='red'),
                    #marker=dict(color='red', size=10),
                    name='Finish Real'
                ))

                # Fechas de colocación y entrega
                fecha_colocacion = pd.to_datetime(df['F_EMISION'].iloc[2])
                fecha_entrega = pd.to_datetime(df['F_ENTREGA'].iloc[2])

                # Agregar líneas verticales para las fechas de colocación y entrega
                fig.add_shape(
                    type="line",
                    x0=fecha_colocacion,
                    y0=0,
                    x1=fecha_colocacion,
                    y1=len(df_gantt),
                    line=dict(color="green", width=2, dash="dash"),
                    name="Fecha Colocación"
                )
		
                # Para la fecha de colocación
                fig.add_annotation(
                    x=fecha_colocacion,
                    y=len(df_gantt)/2,
                    text="Emision<br>" + fecha_colocacion.strftime('%b %d'),
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
		# Para la fecha de entrega
                fig.add_annotation(
                    x=fecha_entrega,
                    y=len(df_gantt)/2,
                    text="Entrega<br>" + fecha_entrega.strftime('%b %d'),
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

                fig.add_shape(
                    type="line",
                    x0=inicial,
                    y0=0,
                    x1=inicial,
                    y1=len(df_gantt),
                    line=dict(color="green", width=2, dash="dash"),
                    name="inicial"
                )
		
                # Para la fecha de colocación
                fig.add_annotation(
                    x=inicial,
                    y=len(df_gantt)/3,
                    text="Inicio<br>" + inicial.strftime('%b %d'),
                    showarrow=True,
                    arrowhead=1
                )        	
                fig.add_shape(
                    type="line",
                    x0=fin,
                    y0=0,
                    x1=fin,
                    y1=len(df_gantt),
                    line=dict(color="red", width=2, dash="dash"),
                    name="fin"
                )
		# Para la fecha de entrega
                fig.add_annotation(
                    x=fin,
                    y=len(df_gantt)/3,
                    text="Fin<br>" + fin.strftime('%b %d'),
                    showarrow=True,
                    arrowhead=1
                )

        

               
                #st.title(f"Pedido: {df['PEDIDO'].iloc[0]}")
                #st.write(f"Cliente: {df['CLIENTE'].iloc[0]}")
                st.plotly_chart(fig)

        except Exception as e:
            st.error(f"Error al ejecutar la consulta: {e}")
    else:
        st.warning("Por favor ingresa un número de pedido.")
