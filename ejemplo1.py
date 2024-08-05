import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sodapy import Socrata
from concurrent.futures import ThreadPoolExecutor, as_completed
import folium
from tabulate import tabulate

# Cliente de Socrata para la API de datos.gov.co
cliente = Socrata('www.datos.gov.co', None)

# Formato esperado 'YYYY-MM-DD'
fecha_inicio = '2024-01-01'
fecha_fin = '2024-12-31'

# Departamentos a filtrar
departamentos = ["CUNDINAMARCA", "BOYACÁ"]

# Consulta SQL para filtrar por fecha y departamento, ordenando por fecha de observación de manera descendente
consulta = f"fechaobservacion between '{fecha_inicio}' and '{fecha_fin}' AND departamento in {tuple(departamentos)} ORDER BY fechaobservacion DESC"

# IDs de los datasets
dataset_ids = ["sbwg-7ju4", "sgfv-3yp8", "s54a-sgyg", "uext-mhny"]

# Función para realizar la solicitud a la API
def fetch_data(dataset_id):
    return cliente.get(dataset_id, where=consulta, limit=500000)

# Diccionario para almacenar los DataFrames resultantes
dataframes = {}

# Usar ThreadPoolExecutor para realizar las solicitudes en paralelo
with ThreadPoolExecutor(max_workers=len(dataset_ids)) as executor:
    # Crear un futuro para cada solicitud
    future_to_dataset_id = {executor.submit(fetch_data, dataset_id): dataset_id for dataset_id in dataset_ids}
    
    # Manejar los resultados a medida que se completen
    for future in as_completed(future_to_dataset_id):
        dataset_id = future_to_dataset_id[future]
        try:
            data = future.result()
            dataframes[dataset_id] = pd.DataFrame.from_records(data)
        except Exception as exc:
            print(f"El dataset {dataset_id} generó una excepción: {exc}")

# Mostrar los primeros registros de cada DataFrame
for dataset_id, df in dataframes.items():
    print(f"\nDataFrame del dataset {dataset_id}:")
    print(df.head())

# Si necesitas hacer algo específico con cada DataFrame, puedes acceder a ellos desde el diccionario `dataframes`
df_temperatura = dataframes.get("sbwg-7ju4")
df_vientos = dataframes.get("sgfv-3yp8")
df_precipitacion = dataframes.get("s54a-sgyg")
df_humedad = dataframes.get("uext-mhny")

# Aquí puedes continuar con el procesamiento y análisis de datos...
