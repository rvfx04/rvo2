import pandas as pd
import plotly.figure_factory as ff
import streamlit as st

# Simulación de los datos de resumen del segundo dataframe
data = {
    "Proceso": ["ARMADO", "TENIDO", "TELAPROB", "CORTE", "COSIDO"],
    "Fecha_inicio": ["2025-01-01", "2025-01-05", "2025-01-10", "2025-01-15", "2025-01-20"],
    "Fecha_fin": ["2025-01-04", "2025-01-09", "2025-01-14", "2025-01-19", "2025-01-25"],
}

df_resumen = pd.DataFrame(data)

# Fechas adicionales
fecha_colocacion = "2025-01-01"
fecha_entrega = "2025-01-26"

# Convertir las fechas a formato datetime
df_resumen["Fecha_inicio"] = pd.to_datetime(df_resumen["Fecha_inicio"])
df_resumen["Fecha_fin"] = pd.to_datetime(df_resumen["Fecha_fin"])
fecha_colocacion = pd.to_datetime(fecha_colocacion)
fecha_entrega = pd.to_datetime(fecha_entrega)

# Crear el gráfico Gantt
fig = ff.create_gantt(
    df_resumen,
    index_col="Proceso",
    show_colorbar=True,
    group_tasks=True,
    colors={"ARMADO": "#636EFA", "TENIDO": "#EF553B", "TELAPROB": "#00CC96", "CORTE": "#AB63FA", "COSIDO": "#FFA15A"},
    bar_width=0.2
)

# Agregar líneas verticales para Fecha_colocacion y F_Entrega
fig.add_vline(
    x=fecha_colocacion,
    line_width=2,
    line_dash="dash",
    line_color="blue",
    annotation_text="Fecha colocación",
    annotation_position="top left",
)

fig.add_vline(
    x=fecha_entrega,
    line_width=2,
    line_dash="dash",
    line_color="red",
    annotation_text="Fecha entrega",
    annotation_position="top left",
)

# Mostrar el gráfico en Streamlit
st.title("Gráfico Gantt de Procesos")
st.plotly_chart(fig)
