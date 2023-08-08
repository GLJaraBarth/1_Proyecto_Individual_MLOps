![Logo](https://blog.soyhenry.com/content/images/2021/02/HEADER-BLOG-NEGRO-01.jpg)

# DATA SCIENCE - PROYECTO INDIVIDUAL Nº1
# Machine Learning Operations (MLOps)


Este proyecto abarca una serie de pasos para desarrollar un proceso de **`Data Engineering`** sobre un dataset de videojuegos de una plataforma multinacional y obtener un **`MVP`**  para posteriormente disponibilizar una serie de endpoints y un modelo de predicción de precio de un video juego utilizando **`Machine Learning`** a través de una **`API`**.

![Logo](https://user-images.githubusercontent.com/67664604/217914153-1eb00e25-ac08-4dfa-aaf8-53c09038f082.png)

## Contexto

Se plantea desde los departamentos de Machine Learning y Analytics la necesidad de contar con los datos en una API para poder ser consumidos.
Por otro lado, existe la necesidad de poder realizar las consultas al modelo de predicción a través de la API para solucionar un problema de negocio.

## Dataset

El [dataset](https://github.com/GLJaraBarth/1_Proyecto_Individual_MLOps/blob/main/steam_games.json) entregado posee información acerca de videojuegos y distintos atributos de los mismos. El mismo cuenta con 32135 registros (representando cada fila un videojuego) y 16 campos (atributos de cada video juego).

## Data Engineering para Data Analisys

Para el trabajo de **`Data Engineering`** se realizó, en líneas generales, el siguiente flujo de trabajo:

- Lectura del archivo JSON y creación del Data Frame.

- Consulta de características y calidad de datos.

- Identificación de features importantes.

- Tratamiento de valores faltantes y posibles errores.

- Transformaciones (Cambio de tipos de dato y desanidados)

- Imputaciones.

- Exportación de datos a un archivo parquet.

El detalle y código en:
[archivo](https://github.com/GLJaraBarth/1_Proyecto_Individual_MLOps/blob/main/steam_games_EDA_DE.ipynb)


## Desarrollo API

Se disponibilizaron los siguientes endpoints a través del Framework **`FastAPI`**:

- def genero( Año: str ): Se ingresa un año y devuelve una lista con los 5 géneros más vendidos en el orden correspondiente. 
  
  p.e. genero '2010' en string y retorna {'Anio': 2010,'Generos': ['Action', 'Indie', 'Strategy', 'Adventure', 'Casual']}

- def juegos( Año: str ): Se ingresa un año y devuelve una lista con los juegos lanzados en el año. 
 
  p.e. juegos '2019' en sring y retorna {'Anio': 2019,'Juegos': ['Raji: An Ancient Epic',"K'NOSSOS",'The Legendary Player - Make Your Reputation - OPEN BETA','The End of an Age: Fading Remnants']}

- def specs( Año: str ): Se ingresa un año y devuelve una lista con los 5 specs que más se repiten en el mismo en el orden correspondiente. 
  
  p.e. specs '2018' en string y retorna {'Anio': 2018,'Specs': ['Single-player','Steam Achievements','Full controller support','Steam Cloud','Partial Controller Support']}

- def earlyacces( Año: str ): Cantidad de juegos lanzados en un año con early access. 
  
  p.e. earlyacces '2013' en string y retorna {'Anio': 2013, 'Cantidad de Juegos con early access': 10}

- def sentiment( Año: str ): Según el año de lanzamiento, se devuelve una lista con la cantidad de registros que se encuentren categorizados con   un análisis de sentimiento.

                    Ejemplo de retorno: {Mixed = 182, Very Positive = 120, Positive = 278}

    p.e. sentiment '2018' en string y retorna {'Anio': 2018,'Cantidad de registros categorizados': {'Sin Datos': 146,'Mixed': 6,'Mostly Positive': 3,'Very Positive': 3,'Mostly Negative': 1}}

- def metascore( Año: str ): Top 5 juegos según año con mayor metascore. 
  
  p.e.metascore '2015' en string y retorna {'Anio': 2015,'Top 5 de juegos   con mayor metascore': ['Grand Theft Auto V','Divinity: Original Sin - Enhanced Edition','Undertale','METAL GEAR SOLID V: THE PHANTOM PAIN','Pillars of Eternity']}

El código para correr la API dentro de FastAPI se puede visualizar [aquí](https://github.com/GLJaraBarth/1_Proyecto_Individual_MLOps/blob/main/main.py) 

## Data Engineering para Machine Learning

El análisis exploratorio de datos **`EDA`** para el modelo de **`Machine Learning`** se realizó, en líneas generales, en el siguiente flujo de trabajo:

- Lectura del archivo PARQUET y creación del Data Frame.

- Identificación de features importantes (Eliminación de irrelevantes y/o redundantes).

- Transformaciones (Cambio de tipo de datos, One-Hot Encoding, Label Encoder)

- Estadísticas descriptivas (Detección de outliers).

- Tratamiento de Outliers (Para asumir valores atípicos, se usa la Regla de las Tres Sigmas).

- Análisis de variables (Mediante Histogramas, Boxplots y Scatterplots).

- Matrices de correlación.

- Exportación.

El detalle y código en:
[archivo](https://github.com/GLJaraBarth/1_Proyecto_Individual_MLOps/blob/main/steam_games_EDA_ML.ipynb)

## Modelo de predicción - Machine Learning

Para el modelo de predicción en **`Machine Learning`** se utilizó como criterio filtrar el dataset algunos de los géneros (6 en total) y el atributo earlyacces (Se asumió que el modelo debe predecir el precio de un nuevo juego, del cual no se cuenta con información de sentiment, metascore, etc.).
Se trata de un modelo de Aprendizaje Supervisado el cual se trabajó con la librería **`Scikit-Learn`** con un algoritmo de regresión lineal múltiple.
El set de datos se dividió en 80% para entrenamiento y 20% para testeo. Para la evaluación del modelo se utilizó en RSME.

- def predicción( genero, earlyaccess = True/False): Ingresando estos parámetros, deberíamos recibir el precio y RMSE. p.e. genero 'Action', earlyaccess = True retorna {'Precio': 10.95969009399414, 'RSME': 7.25}

El detalle y código en:
[archivo](https://github.com/GLJaraBarth/1_Proyecto_Individual_MLOps/blob/main/modelo_ML.ipynb)

## Deployment

Para el deploy de la API, se utilizó la plataforma **`Render`**.
Los datos están listos para ser consumidos y consultados a partir del siguiente link

[Link al Deployment](https://gljara-1-proyecto-mlops.onrender.com/docs#)

## Video 

En el [enlace](https://youtu.be/DUCHEEekHiw) se puede acceder a video expicativo del proyecto.
