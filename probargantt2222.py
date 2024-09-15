# Función para crear el gráfico de Gantt
import plotly.graph_objs as go
from datetime import datetime
import pandas as pd
import streamlit as st



import plotly.graph_objs as go
from datetime import datetime
import pandas as pd
import streamlit as st

def create_gantt(df, f_emision, f_entrega):
    # Crear el gráfico de Gantt
    fig = go.Figure()
    
    # Iterar sobre los procesos y agregar trazas al gráfico
    for i, row in df.iterrows():
        fecha_inicio = row['fecha_inicio']
        fecha_fin = row['fecha_fin']
        if fecha_inicio >= fecha_fin:
            st.write(f"Error: La fecha de inicio ({fecha_inicio}) es mayor o igual a la fecha de fin ({fecha_fin}) para el proceso {row['proceso']}")
            continue
        duracion = (fecha_fin - fecha_inicio).days
        st.write(f"Proceso: {row['proceso']}, Fecha Inicio: {fecha_inicio}, Fecha Fin: {fecha_fin}, Duración: {duracion} días")
        
        # Agregar la barra de Gantt para cada proceso
        fig.add_trace(go.Bar(
            x=[fecha_inicio, fecha_fin],  # Usar fecha_inicio y fecha_fin directamente
            y=[row['proceso'], row['proceso']],  # Repetir el proceso para mantener la barra horizontal
            orientation='h',
            text=f"{row['proceso']}: {row['progreso']}%",
            hoverinfo='text',
            marker=dict(color='skyblue'),
            showlegend=False,
            name=row['proceso']
        ))

    # Trazar la fecha de emisión y la fecha de entrega usando add_shape()
    fig.add_shape(
        type="line",
        x0=f_emision, x1=f_emision,
        y0=-0.5, y1=len(df)-0.5,
        line=dict(color="green", width=2, dash="dash"),
        name="F. Emisión"
    )
    fig.add_shape(
        type="line",
        x0=f_entrega, x1=f_entrega,
        y0=-0.5, y1=len(df)-0.5,
        line=dict(color="red", width=2, dash="dash"),
        name="F. Entrega"
    )

    # Trazar el día actual
    dia_actual = datetime.today().date()
    fig.add_shape(
        type="line",
        x0=dia_actual, x1=dia_actual,
        y0=-0.5, y1=len(df)-0.5,
        line=dict(color="blue", width=2, dash="dash"),
        name="Hoy"
    )

    # Configuración del eje X (fechas) y eje Y (procesos)
    fig.update_layout(
        title="Gráfico de Gantt - Procesos de Producción",
        xaxis_title="Fecha",
        yaxis_title="Proceso",
        xaxis=dict(
            type='date',
            tickformat='%d-%m-%Y',
            dtick="D7",  # Mostrar ticks cada 7 días
            range=[f_emision, f_entrega]  # Establecer el rango del eje X
        ),
        yaxis=dict(categoryorder="array", categoryarray=df['proceso']),
        barmode='overlay',  # Asegura que las barras se superpongan correctamente
        bargap=0.2,
        height=400 + (len(df) * 30),  # Ajustar la altura del gráfico según el número de procesos
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )

    # Agregar leyenda para las líneas de fecha
    fig.add_trace(go.Scatter(x=[None], y=[None], mode='lines', line=dict(color='green', dash='dash'), name='F. Emisión'))
    fig.add_trace(go.Scatter(x=[None], y=[None], mode='lines', line=dict(color='red', dash='dash'), name='F. Entrega'))
    fig.add_trace(go.Scatter(x=[None], y=[None], mode='lines', line=dict(color='blue', dash='dash'), name='Hoy'))

    # Mostrar el gráfico en Streamlit
    st.plotly_chart(fig)

# Datos de prueba ajustados
df = pd.DataFrame({
    'proceso': ['ARM', 'TENID', 'TELAPROB', 'CORTADO', 'COSIDO'],
    'fecha_inicio': ['2024-07-01', '2024-07-10', '2024-07-20', '2024-08-01', '2024-08-15'],
    'fecha_fin': ['2024-07-05', '2024-07-15', '2024-07-25', '2024-08-05', '2024-08-20'],
    'progreso': [100, 80, 60, 90, 75]
})

# Convertir las fechas a datetime en el DataFrame
df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'])
df['fecha_fin'] = pd.to_datetime(df['fecha_fin'])

# Definir F_EMISION y F_ENTREGA
f_emision = datetime(2024, 6, 28)  # Ejemplo de fecha de emisión
f_entrega = datetime(2024, 8, 25)  # Ejemplo de fecha de entrega

# Título de la aplicación
st.title("Gráfico de Gantt - Proceso por Pedido")

# Llamar a la función para crear el gráfico
create_gantt(df, f_emision, f_entrega)
