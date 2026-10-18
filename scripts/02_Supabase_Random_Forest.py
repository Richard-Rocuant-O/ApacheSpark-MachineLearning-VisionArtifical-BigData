# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.6
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Evaluación 2 · Big Data — Notebook 02: integración con Supabase y modelo predictivo
#
# **Opción C (entorno híbrido):** Google Colab + PySpark + Supabase (PostgreSQL) · PySpark 3.5.3.
#
# | | |
# |---|---|
# | **Objetivo** | Integrar el procesamiento con una base de datos en la nube y entrenar un clasificador de ejercicios a partir de los puntos corporales estimados. |
# | **Entrada** | `dataset_procesado.zip` (generado por el notebook 01): 3.501 registros en 9 archivos Parquet. |
# | **Salida** | Tabla `resumen_ejercicios` en Supabase y un modelo Random Forest (PySpark MLlib) con sus métricas de evaluación. |
# | **Flujo** | ZIP → lectura con Spark → resumen en PostgreSQL → extracción de 132 características de pose → entrenamiento → evaluación. |
#
# **Requisitos (Colab).**
# 1. El archivo `dataset_procesado.zip`, que se selecciona en la celda 11.1.
# 2. El secreto `SUPABASE_DB_URL` con la URI del *Session pooler* de Supabase (panel *Secretos*) y el acceso habilitado para este cuaderno. La URI no se escribe en el código ni se imprime.
#
# **Ejecución local.** Definir la variable de entorno `SUPABASE_DB_URL` y ubicar el ZIP en `proyecto_big_data/processed/`.
#
# Las secciones usan la misma numeración del informe técnico. La última celda contrasta los resultados con los valores de referencia.
# %% [markdown]
# ## 5.2 Dependencias y sesión de Spark

# %%
# PySpark fijado en 3.5.3 (versión del informe). psycopg es el cliente de PostgreSQL.
# setuptools: PySpark 3.5 usa `distutils`, que Python 3.12 ya no incluye.
# %pip install -q pyspark==3.5.3 pyarrow "psycopg[binary]" setuptools

# %%
import os, sys, shutil, zipfile
from pathlib import Path
import pyspark
from pyspark.sql import SparkSession, functions as F

EN_COLAB = "google.colab" in sys.modules                                    # True si corre en Google Colab
BASE = Path("/content") if EN_COLAB else Path.cwd() / "proyecto_big_data"   # en Colab los archivos viven en /content
BASE.mkdir(parents=True, exist_ok=True)

# Spark necesita Java 8/11/17. Colab ya lo trae; en local debe existir JAVA_HOME o `java` en el PATH.
if not (shutil.which("java") or os.environ.get("JAVA_HOME")):
    raise EnvironmentError("No se encontró Java. Instala JDK 17 y define JAVA_HOME.")
os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable   # mismo Python para los procesos de Spark

# Se fijan 2 núcleos (los del Colab gratuito): randomSplit reparte las filas según las particiones de Spark, y estas
# dependen de los núcleos. Con 2 núcleos la división train/test sale igual en cualquier máquina.
spark = (SparkSession.builder.appName("Evaluacion2_BigData_02").master("local[2]")
         .config("spark.sql.shuffle.partitions", "4").config("spark.driver.memory", "2g").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print("PySpark", pyspark.__version__, "| Spark", spark.version, "|", spark.sparkContext.master)

# %% [markdown]
# ## 11.1 Carga del ZIP y lectura con Spark

# %%
# 1) Obtener el ZIP: en Colab se elige desde el equipo local; en local se busca en processed/
if EN_COLAB:
    from google.colab import files
    subidos = files.upload()                                                # abre el selector de archivos
    nombre = next((n for n in subidos if n.lower().endswith(".zip")), None)
    ruta_zip = BASE / nombre if nombre else None
else:
    ruta_zip = BASE / "processed" / "dataset_procesado.zip"
if ruta_zip is None or not ruta_zip.exists():
    raise FileNotFoundError("Falta dataset_procesado.zip (lo genera el notebook 01).")

# 2) Extraer comprobando que ningún archivo del ZIP se salga de la carpeta de destino
destino = BASE / "dataset_procesado"
if destino.exists():
    shutil.rmtree(destino)
destino.mkdir(parents=True)
with zipfile.ZipFile(ruta_zip) as z:
    for miembro in z.infolist():
        if not (destino / miembro.filename).resolve().is_relative_to(destino.resolve()):
            raise ValueError("El ZIP contiene una ruta no permitida.")
    z.extractall(destino)

# 3) Leer con Spark. Se pasan los archivos uno a uno (en Windows Spark no puede listar carpetas sin winutils);
#    basePath le permite reconstruir la columna "label" a partir de los nombres de carpeta.
archivos = sorted(destino.rglob("*.parquet"))
if not archivos:
    raise FileNotFoundError("No hay archivos Parquet dentro del ZIP.")
df_cloud = spark.read.option("basePath", str(destino)).parquet(*[str(a) for a in archivos])

n_total = df_cloud.count()
print("Archivos Parquet:", len(archivos), "| registros:", n_total, "| columnas:", len(df_cloud.columns))   # esperado: 9 | 3501 | 7
df_cloud.groupBy("label").count().orderBy(F.desc("count"), "label").show(truncate=False)

# %% [markdown]
# ## 11.2 Conexión a Supabase (5.5 Permisos y credenciales)

# %%
import psycopg
from urllib.parse import urlparse

def url_bd():
    """URI de conexión: secreto de Colab o variable de entorno. Nunca se escribe en el código ni se imprime."""
    if EN_COLAB:
        from google.colab import userdata
        return userdata.get("SUPABASE_DB_URL")      # si falta el secreto o el acceso, Colab lo indica con un error claro
    return os.environ["SUPABASE_DB_URL"]

DATABASE_URL = url_bd()
datos = urlparse(DATABASE_URL)
# Solo host, puerto y usuario (nunca la contraseña). Con el Session pooler: host "...pooler.supabase.com", usuario "postgres.<ref>".
print("Host:", datos.hostname, "| puerto:", datos.port, "| usuario:", datos.username)

SSLMODE = os.environ.get("DB_SSLMODE", "require")   # Supabase exige SSL; solo se cambia para probar con un Postgres local
with psycopg.connect(DATABASE_URL, connect_timeout=30, sslmode=SSLMODE) as conn, conn.cursor() as cur:
    cur.execute("SELECT current_database(), current_user")
    print("Conexión exitosa. Base de datos y usuario:", cur.fetchone())

# %% [markdown]
# ## 11.3 Resumen por ejercicio en Supabase

# %%
import pandas as pd

# Resumen calculado con Spark: una fila por ejercicio (9 filas)
resumen = [(f["label"], int(f["cantidad"])) for f in df_cloud.groupBy("label").agg(F.count("*").alias("cantidad")).collect()]

with psycopg.connect(DATABASE_URL, connect_timeout=30, sslmode=SSLMODE) as conn, conn.cursor() as cur:
    cur.execute("""CREATE TABLE IF NOT EXISTS resumen_ejercicios (
                       ejercicio TEXT PRIMARY KEY,
                       cantidad  INTEGER NOT NULL)""")
    # Seguridad por filas activada: nadie accede a la tabla por la API pública de Supabase.
    # Esta conexión directa usa el rol postgres y no se ve afectada.
    cur.execute("ALTER TABLE resumen_ejercicios ENABLE ROW LEVEL SECURITY")
    # Si el ejercicio ya existe se actualiza su cantidad (no se suma)
    cur.executemany("""INSERT INTO resumen_ejercicios (ejercicio, cantidad) VALUES (%s, %s)
                       ON CONFLICT (ejercicio) DO UPDATE SET cantidad = EXCLUDED.cantidad""", resumen)
    # Se borran categorías antiguas que ya no estén en el resumen actual
    cur.execute("DELETE FROM resumen_ejercicios WHERE NOT (ejercicio = ANY(%s))", ([e for e, _ in resumen],))

# Al salir del bloque anterior la transacción se confirma (commit). La verificación se hace con una conexión NUEVA,
# para comprobar lo que realmente quedó guardado en la base y no solo lo visible dentro de la transacción.
with psycopg.connect(DATABASE_URL, connect_timeout=30, sslmode=SSLMODE) as conn, conn.cursor() as cur:
    cur.execute("SELECT ejercicio, cantidad FROM resumen_ejercicios ORDER BY cantidad DESC, ejercicio")
    guardado = cur.fetchall()

df_resumen = pd.DataFrame(guardado, columns=["ejercicio", "cantidad"])
display(df_resumen)
print("Categorías:", len(df_resumen), "| total de registros:", int(df_resumen["cantidad"].sum()))   # esperado: 9 | 3501
assert int(df_resumen["cantidad"].sum()) == n_total, "El resumen guardado no suma los registros del ZIP"

# %% [markdown]
# ## 8.1–8.4 Preparación de las poses

# %%
# poseLandmarks es NULL en 860 imágenes y no sirve para el modelo (el resumen de Supabase sí las cuenta todas).
# La etiqueta pasa a mayúscula inicial: "shoulder press" -> "Shoulder Press".
df_preparado = (df_cloud
                .withColumn("label", F.initcap(F.col("label")))
                .filter(F.col("poseLandmarks").isNotNull() & (F.length(F.trim("poseLandmarks")) > 0)))
n_poses = df_preparado.count()
print("Registros:", n_total, "| con pose:", n_poses, "| excluidos:", n_total - n_poses)   # esperado: 3501 | 2641 | 860
df_preparado.groupBy("label").count().orderBy(F.desc("count"), "label").show(truncate=False)

# %% [markdown]
# ## 8.5–8.6 Características numéricas

# %%
import setuptools                                   # restituye `distutils`, que PySpark 3.5 necesita en Python 3.12
from pyspark.ml.functions import array_to_vector

# poseLandmarks es un texto como "['x: 0.79,y: 0.28,z: -0.73,visibility: 0.99,', ...]" (33 puntos de MediaPipe).
# No es JSON: se extraen los números con una expresión regular de Spark. Cada punto trae x, y, z y visibility
# en ese orden, así que la lista queda [x1, y1, z1, v1, x2, ...]. Se usa una función nativa de Spark (no un UDF
# de Python): es más rápida y funciona igual en Windows y en Colab.
NUMERO = "-?(?:[0-9]+(?:[.][0-9]*)?|[.][0-9]+)(?:[eE][+-]?[0-9]+)?"
df_car = (df_preparado
          .withColumn("features_array",
                      F.expr("transform(regexp_extract_all(poseLandmarks, '(?:x|y|z|visibility): *(" + NUMERO + ")', 1),"
                             " v -> cast(v as double))"))
          .filter(F.size("features_array") > 0))

# Solo se conservan las poses con la longitud más frecuente (33 puntos x 4 valores = 132)
distribucion = df_car.groupBy(F.size("features_array").alias("longitud")).count().orderBy(F.desc("count"), F.desc("longitud"))
distribucion.show()
longitud = distribucion.first()["longitud"]

df_modelo = (df_car.filter(F.size("features_array") == longitud)
                   .withColumn("features", array_to_vector("features_array"))   # lista de números -> vector de MLlib
                   .select("label", "features").cache())
print("Valores por pose:", longitud, "| registros para el modelo:", df_modelo.count())   # esperado: 132 | 2641

# %% [markdown]
# ## 8.7–8.9 División train/test y entrenamiento

# %%
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import RandomForestClassifier

SEMILLA = 42
# División 80/20 al azar por filas. No es estratificada y no agrupa imágenes repetidas (hay 116 en el CSV),
# así que la misma foto puede quedar en entrenamiento y en prueba y la exactitud podría ser optimista.
entrenamiento, prueba = df_modelo.randomSplit([0.8, 0.2], seed=SEMILLA)
n_train, n_test = entrenamiento.count(), prueba.count()
print("Entrenamiento:", n_train, "| prueba:", n_test)   # referencia: 2145 | 496

modelo = Pipeline(stages=[
    StringIndexer(inputCol="label", outputCol="label_index", handleInvalid="skip"),       # texto -> número
    RandomForestClassifier(featuresCol="features", labelCol="label_index", numTrees=50, maxDepth=10, seed=SEMILLA),
]).fit(entrenamiento)                                # el ajuste usa solo los datos de entrenamiento
predicciones = modelo.transform(prueba).cache()
print("Predicciones:", predicciones.count())

# %% [markdown]
# ## 11.5 Evaluación del modelo

# %%
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import IndexToString

metricas = {}
for nombre, metrica in (("Accuracy", "accuracy"), ("Precisión ponderada", "weightedPrecision"),
                        ("Recall ponderado", "weightedRecall"), ("F1 ponderado", "f1")):
    evaluador = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName=metrica)
    metricas[nombre] = evaluador.evaluate(predicciones)
    print(f"{nombre}: {metricas[nombre]:.4f}")

# Nombres de ejercicio en lugar de índices numéricos
etiquetas = modelo.stages[0].labels
pred = IndexToString(inputCol="prediction", outputCol="predicho", labels=etiquetas).transform(predicciones)

# Matriz de confusión: filas = ejercicio real, columnas = ejercicio predicho (los aciertos están en la diagonal)
pred.groupBy("label").pivot("predicho", etiquetas).count().na.fill(0).orderBy("label").show(truncate=False)

# %%
# Recall y precisión por ejercicio: qué ejercicios se reconocen mejor y cuáles peor
real = pred.groupBy("label").agg(F.count("*").alias("soporte"),
                                 F.sum((F.col("label") == F.col("predicho")).cast("int")).alias("aciertos"))
predichos = pred.groupBy(F.col("predicho").alias("label")).agg(F.count("*").alias("predichos"))
(real.join(predichos, "label", "left")
     .select("label", "soporte",
             F.round(F.col("aciertos") / F.col("soporte"), 3).alias("recall"),
             F.round(F.col("aciertos") / F.col("predichos"), 3).alias("precision"))
     .orderBy("recall").show(truncate=False))

# %% [markdown]
# ## Verificación de resultados

# %%
# (nombre, obtenido, valor de referencia, tolerancia). Las cuentas deben ser exactas; en las métricas se toleran
# 3 puntos porque el azar de Spark (división y bosque) puede variar entre entornos.
chequeos = [
    ("Registros leídos del ZIP",      n_total,                          3501,   0),
    ("Archivos Parquet",              len(archivos),                    9,      0),
    ("Total guardado en Supabase",    int(df_resumen["cantidad"].sum()), 3501,   0),
    ("Registros con pose",            n_poses,                          2641,   0),
    ("Valores por pose",              longitud,                         132,    0),
    ("Entrenamiento",                 n_train,                          2145,   0),
    ("Prueba",                        n_test,                           496,    0),
    ("Accuracy",                      metricas["Accuracy"],             0.6875, 0.03),
    ("F1 ponderado",                  metricas["F1 ponderado"],         0.6855, 0.03),
]
print(f"{'Paso':32}{'Obtenido':>10}{'Referencia':>12}")
todo_ok = True
for nombre, obtenido, esperado, tolerancia in chequeos:
    ok = abs(obtenido - esperado) <= tolerancia
    todo_ok &= ok
    print(f"{nombre:32}{obtenido:>10.4f}{esperado:>12}  {'✔' if ok else '✘'}")
print("\nTodos los resultados coinciden con los valores de referencia." if todo_ok else "\nHay diferencias: revisar las filas marcadas con ✘.")

# %% [markdown]
# ## Muestras para el informe (opcional)
# Esta celda **no es necesaria para el pipeline**. Imprime salidas representativas (esquema, texto de `poseLandmarks`,
# números extraídos y predicciones con probabilidad) para ilustrar el informe técnico (figuras 8.2, 8.4, 8.6, 8.8 y 11.36).
# Puede omitirse.

# %%
# Figura 8.2 — esquema del DataFrame: las 7 columnas con su tipo
print("Esquema del conjunto de datos:")
df_cloud.printSchema()

# Figura 8.2 — 3 filas de poseLandmarks tal como vienen (texto con los puntos del cuerpo)
print("\nEjemplos de poseLandmarks:")
df_cloud.select("poseLandmarks").filter(F.col("poseLandmarks").isNotNull()).show(3, truncate=1000)

# Figura 8.4 — 5 filas del mismo texto después de excluir las poses vacías
print("\nEjemplos del formato de poseLandmarks (ya preparadas):")
df_preparado.select("poseLandmarks").show(5, truncate=500)

# Figura 8.6 — un registro completo en vertical, y las 3 primeras listas de números ya extraídos
print("\nEjemplo original:")
df_preparado.select("poseLandmarks").show(1, truncate=2000, vertical=True)
print("\nCaracterísticas extraídas (label y lista de 132 números):")
df_car.select("label", "features_array").show(3, truncate=300)

# Figura 8.8 — 10 predicciones con su índice y la probabilidad de cada clase
print("\nPredicciones con probabilidad:")
predicciones.select("label", "label_index", "prediction", "probability").show(10, truncate=False)

# Figura 11.36 — 15 ejemplos: ejercicio real frente al predicho
print("\nEjemplos de predicciones:")
pred.select("label", "predicho").show(15, truncate=False)
