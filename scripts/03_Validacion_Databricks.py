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
# # Evaluación 2 · Big Data — Notebook 03: validación del dataset en Databricks
#
# **Opción C (entorno híbrido):** Databricks Free Edition (cómputo *serverless*) · Unity Catalog Volumes.
#
# | | |
# |---|---|
# | **Objetivo** | Comprobar que el dataset procesado puede cargarse y consultarse con Spark en una segunda plataforma en la nube. |
# | **Entrada** | `dataset_procesado.zip` (generado por el notebook 01), almacenado en un *Volume* de Unity Catalog. |
# | **Salida** | Validación de los registros, columnas y categorías del dataset leído con Spark. |
#
# **Requisitos.** Un volumen con el ZIP cargado (ruta `/Volumes/<catálogo>/<esquema>/<volumen>`). Si los nombres difieren de `workspace` / `default` / `dataset`, ajustar `VOLUMEN` en la primera celda de código. En Databricks `spark` ya existe: el notebook no crea ninguna sesión.
#
# Las secciones usan la numeración del informe técnico (11.2). La última celda contrasta los resultados con los valores de referencia.
# %% [markdown]
# ## 11.2 Extracción del ZIP en el Volume

# %%
import zipfile
from pathlib import Path
from pyspark.sql import functions as F

# Ruta del volumen: /Volumes/<catálogo>/<esquema>/<volumen>
VOLUMEN = Path("/Volumes/workspace/default/dataset")
RUTA_ZIP = VOLUMEN / "dataset_procesado.zip"
RUTA_DESTINO = VOLUMEN / "extraido"

if not RUTA_ZIP.is_file():
    raise FileNotFoundError(f"No se encontró el ZIP en el volumen: {RUTA_ZIP}")

# Si la carpeta de destino ya existe (por una ejecución anterior) se reutiliza: no se intenta borrarla.
RUTA_DESTINO.mkdir(parents=True, exist_ok=True)

# Extraer comprobando que ningún archivo del ZIP se salga de la carpeta de destino
with zipfile.ZipFile(RUTA_ZIP) as z:
    for miembro in z.infolist():
        if not (RUTA_DESTINO / miembro.filename).resolve().is_relative_to(RUTA_DESTINO.resolve()):
            raise ValueError("El ZIP contiene una ruta no permitida.")
    z.extractall(RUTA_DESTINO)
    # Solo se leerán los Parquet que trae ESTE ZIP (no los restos de ejecuciones anteriores que queden en la carpeta).
    archivos = sorted(RUTA_DESTINO / nombre for nombre in z.namelist() if nombre.endswith(".parquet"))
if not archivos:
    raise FileNotFoundError("No hay archivos Parquet dentro del ZIP.")
print("Archivos Parquet:", len(archivos))   # esperado: 9

# %% [markdown]
# ## 11.2 Lectura con Spark y validación

# %%
# Se leen los archivos uno a uno (igual que en los notebooks 01 y 02); basePath reconstruye la columna "label"
# a partir de los nombres de carpeta.
df_databricks = spark.read.option("basePath", str(RUTA_DESTINO)).parquet(*[str(a) for a in archivos])

n_total = df_databricks.count()
print("Registros:", n_total, "| columnas:", df_databricks.columns)   # esperado: 3501 | 7 columnas
df_databricks.printSchema()

# Distribución por ejercicio (cantidad y porcentaje)
df_dist = (df_databricks.groupBy("label").agg(F.count("*").alias("cantidad"))
           .withColumn("porcentaje", F.round(F.col("cantidad") * 100 / n_total, 2))
           .orderBy(F.desc("cantidad"), "label"))
df_dist.show(truncate=False)

# %% [markdown]
# ## Verificación de resultados

# %%
columnas_esperadas = {"imageName", "base64encoded", "imageWidth", "imageHeight", "poseLandmarks", "excercise", "label"}
cantidades = {f["label"]: f["cantidad"] for f in df_dist.collect()}

# (nombre, obtenido, valor de referencia)
chequeos = [
    ("Archivos Parquet",       len(archivos),                                   9),
    ("Registros",              n_total,                                         3501),
    ("Columnas",               len(df_databricks.columns),                      7),
    ("Columnas esperadas",     int(set(df_databricks.columns) == columnas_esperadas), 1),
    ("Categorías",             len(cantidades),                                 9),
    ("Glute bridge",           cantidades.get("glute bridge"),                  314),
    ("Chest dips",             cantidades.get("chest dips"),                    389),
    ("Pushup",                 cantidades.get("pushup"),                        398),
]
print(f"{'Paso':24}{'Obtenido':>10}{'Referencia':>12}")
todo_ok = True
for nombre, obtenido, esperado in chequeos:
    ok = obtenido == esperado
    todo_ok &= ok
    print(f"{nombre:24}{str(obtenido):>10}{esperado:>12}  {'✔' if ok else '✘'}")
print("\nTodos los resultados coinciden con los valores de referencia." if todo_ok else "\nHay diferencias: revisar las filas marcadas con ✘.")
