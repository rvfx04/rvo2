import streamlit as st
import xml.etree.ElementTree as ET
import pandas as pd

# Función para procesar cada archivo XML
def procesar_xml(xml_file, file_name):
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Diccionario para almacenar la información básica
    info = {
        "File Name": file_name,
        "Operating System": None,
        "CPU": None,
        "RAM": None,
        "Motherboard": None,
        "Graphics": None,
        "Storage": None,
        "Audio": None
    }

    # Buscar la sección 'Summary' y extraer la información básica, teniendo en cuenta las traducciones
    for main_section in root.findall("mainsection"):
        if main_section.attrib.get('title') in ['Summary', 'Resumen']:
            for section in main_section.findall("section"):
                section_title = section.attrib.get('title')
                for entry in section.findall('entry'):
                    title = entry.attrib.get('title')

                    # Agrupamos por posibles traducciones
                    if section_title in ["Operating System", "Sistema Operativo"]:
                        if title in ["Windows 11 Home 64-bit", "Windows 11 Home 64-bit"]:  # Puede haber traducciones aquí
                            info["Operating System"] = title
                    elif section_title in ["CPU", "Procesador"]:
                        info["CPU"] = entry.attrib.get('title')
                    elif section_title in ["RAM", "Memoria RAM"]:
                        info["RAM"] = entry.attrib.get('title')
                    elif section_title in ["Motherboard", "Placa Madre"]:
                        info["Motherboard"] = entry.attrib.get('title')
                    elif section_title in ["Graphics", "Gráficos"]:
                        info["Graphics"] = entry.attrib.get('title')
                    elif section_title in ["Storage", "Almacenamiento"]:
                        info["Storage"] = entry.attrib.get('title')
                    elif section_title in ["Audio", "Sonido"]:
                        info["Audio"] = entry.attrib.get('title')

    return info

# Configuración de la aplicación Streamlit
st.title("Visor de Información de Archivos XML")
st.write("Sube tus archivos XML para extraer y visualizar la información básica en una tabla consolidada.")

# Subir múltiples archivos XML
uploaded_files = st.file_uploader("Sube los archivos XML", type="xml", accept_multiple_files=True)

# Lista donde se almacenará la información procesada de todos los archivos
datos = []

if uploaded_files:
    for file in uploaded_files:
        # Procesar cada archivo XML y agregar la información a la lista (incluyendo el nombre del archivo)
        file_name = file.name
        datos.append(procesar_xml(file, file_name))

    # Convertir la lista en un DataFrame de pandas para visualizarla como tabla
    df = pd.DataFrame(datos)

    # Mostrar la tabla consolidada
    st.write("### Tabla Consolidada de Información Básica")
    st.dataframe(df)

else:
    st.write("No se han subido archivos todavía.")