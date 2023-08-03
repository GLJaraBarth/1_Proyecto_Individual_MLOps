#Importamos las librerias
from fastapi import FastAPI
import pandas as pd 
import numpy as np

#Instanciamos la clase, indicamos título y descripción de la API
app = FastAPI(title='PROYECTO INDIVIDUAL Nº1 -Machine Learning Operations (MLOps) -Guillermo Jara',
            description='API de datos y ML')

#http://127.0.0.1:8000

# Datasets
# Cargar el archivo Parquet en un DataFrame
df_salida = pd.read_parquet('steam_games.parquet')


# Función para reconocer el servidor local

@app.get('/')
async def index():
    return {'Hola! Bienvenido a la API de recomedación. Por favor dirigite a /docs'}

@app.get('/about/')
async def about():
    return {'PROYECTO INDIVIDUAL Nº1 -Machine Learning Operations (MLOps)'}

@app.get('/genero/({Anio})')
def genero(Anio:str): 
    #Se ingresa un año y devuelve una lista con los 5 géneros más vendidos en el orden correspondiente.
    Anio = int(Anio)
    lista_generos = []
    top_generos = []
    df_filtrado_anio = df_salida[df_salida['release_year'] == Anio]

    df_filtrado_anio_unique = df_filtrado_anio.drop_duplicates(subset='id')

    # Agrupar por la columna 'Nombre'
    grupos_genres = df_filtrado_anio_unique.groupby('genres')

    # Calcular la media de edad para cada grupo
    top_genres = grupos_genres['genres'].count()

    top_genres.sort_values(ascending=False, inplace=True)

    # Tomar los primeros 5 elementos de la Serie y almacenarlos en una lista
    lista_generos = top_genres.index.tolist()

    top_generos = lista_generos[:5]

    return {'Anio': Anio, 'Generos': top_generos}

@app.get('/juegos/({Anio})')
def juegos(Anio:str): 
    #Se ingresa un año y devuelve una lista con los juegos lanzados en el año.
    Anio = int(Anio)
    lista_juegos = []
    df_filtrado_anio = df_salida[df_salida['release_year'] == Anio]

    df_filtrado_anio_unique = df_filtrado_anio.drop_duplicates(subset='id')

    lista_juegos = list(df_filtrado_anio_unique['app_name'])

    return {'Anio': Anio, 'Juegos': lista_juegos}

@app.get('/sepcs/({Anio})')
def specs(Anio:str): 
    #Se ingresa un año y devuelve una lista con los 5 géneros más vendidos en el orden correspondiente.
    Anio = int(Anio)
    lista_specs = []
    top_specs = []
    df_filtrado_anio = df_salida[df_salida['release_year'] == Anio]

    df_filtrado_anio_unique = df_filtrado_anio.drop_duplicates(subset='id')

    # Agrupar por la columna 'Nombre'
    grupos_specs = df_filtrado_anio_unique.groupby('specs')

    # Calcular la media de edad para cada grupo
    top_specs = grupos_specs['specs'].count()

    top_specs.sort_values(ascending=False, inplace=True)

    # Tomar los primeros 5 elementos de la Serie y almacenarlos en una lista
    lista_specs = top_specs.index.tolist()

    top_specs = lista_specs[:5]

    return {'Anio': Anio, 'Generos': top_specs}