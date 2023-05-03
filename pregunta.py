"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""

import re
import pandas as pd


RUTA = "clusters_report.txt"


def procesar_porcentaje(texto: str) -> float:

    """
    Conversión de porcentaje
    de formato ("XX,X %") a formato (XX.X)
    """

    porcentaje_coma = texto.split()[0] # Formato ("XX,X")
    porcentaje = float(porcentaje_coma.replace(",", ".")) # Formato (XX.X)

    return porcentaje

def procesar_encabezado(texto: str) -> list:

    """
    Separar las líneas de encabezado de clusters_report.txt
    """

    fixed = texto.lower().replace(" ", "_").replace("\n", "")
    resultado = re.split(string = fixed, pattern = r"_{2,}")

    return resultado

def fusionar_encabezado(linea_1: list, linea_2: list) -> list:

    """
    Fusionar las dos líneas del encabezado de clusters_report.txt
    """

    uniones = len(linea_2)
    header = []

    for num_columna, texto_1 in enumerate(linea_1):

        if num_columna < uniones:
            if linea_2[num_columna] != "":
                elemento = texto_1 + "_" + linea_2[num_columna]
            else:
                elemento = texto_1
        else:
            elemento = texto_1

        header.append(elemento)

    return header

def procesar_palabras (lista: list) -> str:

    """
    Procesamiento de lista de palabras.
    Vuelve a juntar las palabras y quita los puntos
    """

    palabras = ' '.join(lista)
    palabras = palabras.replace(".","")

    return palabras


def ingest_data():

    """
    Lectura del archivo clusters_report.txt como tabla.
    """

    # Iniciar lectura de archivo
    archivo = open(RUTA, mode='r', encoding="utf-8")

    # Procesamiento de encabezado

    encabezado_1_origin = archivo.readline().strip() # Linea 1
    encabezado_1 = procesar_encabezado(texto = encabezado_1_origin)

    encabezado_2_origin = archivo.readline() # Linea 2
    encabezado_2 = procesar_encabezado(texto = encabezado_2_origin)

    encabezado = fusionar_encabezado(linea_1 = encabezado_1, linea_2 = encabezado_2)

    # Creación de dataframe
    df = pd.DataFrame(columns = encabezado)

    # Ignorar lineas
    archivo.readline() # Linea 3 - Vacia
    archivo.readline() # Linea 4 - Guiones

    # Análisis de las líneas de datos
    for linea in archivo:

        linea = linea.strip()

        if linea == "":
            registro = [cluster, cantidad, porcentaje, palabras]
            df.loc[len(df)] = registro

        elif "%" in linea:
            separar = re.split(string = linea, pattern = r"\s{2,}")

            cluster = int(separar[0])
            cantidad = int(separar[1])
            porcentaje = procesar_porcentaje(separar[2])

            palabras_raw = separar[3:]
            palabras = procesar_palabras(palabras_raw)

        else:
            # Quitar espacios más grandes
            separar = re.split(string = linea, pattern = r"\s{2,}")

            palabras_linea = procesar_palabras(separar)

            # Unir las palabras ya obtenidas con las de la línea actual
            palabras = ' '.join([palabras, palabras_linea])

    # Agregar último registro
    registro = [cluster, cantidad, porcentaje, palabras]
    df.loc[len(df)] = registro

    # Cerrar lectura del archivo
    archivo.close()

    return df
