#Cargue de datasets disponibles en datos abiertos.gov.co

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sodapy import Socrata
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

# Petición a la API con la consulta
resul_temper = cliente.get("sbwg-7ju4", where=consulta, limit=500000)
resul_vientos = cliente.get("sgfv-3yp8", where=consulta, limit=500000)
resul_precip = cliente.get("s54a-sgyg", where=consulta, limit=500000)
resul_humedad = cliente.get("uext-mhny", where=consulta, limit=500000)

# Resultados a DataFrame
df_temperatura = pd.DataFrame.from_records(resul_temper)
df_vientos = pd.DataFrame.from_records(resul_vientos)
df_precipitacion = pd.DataFrame.from_records(resul_precip)
df_humedad = pd.DataFrame.from_records(resul_humedad)

print(df_temperatura.head())
print(df_vientos.head())
print(df_precipitacion.head())
print(df_humedad.head())
