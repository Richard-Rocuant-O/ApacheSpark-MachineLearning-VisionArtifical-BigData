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
# # Evaluación 2 · Big Data — Notebook 01: ingesta, limpieza, análisis y almacenamiento
#
# **Opción C (entorno híbrido):** Google Colab + PySpark · Dataset *Exercise Images With Pose Estimation* (Kaggle, asher213) · PySpark 3.5.3.
#
# | | |
# |---|---|
# | **Objetivo** | Preparar el dataset para el análisis y el modelamiento: ingesta, validación de calidad, almacenamiento en formato columnar y análisis descriptivo distribuido. |
# | **Entrada** | `imagePoses.csv` (3.501 registros, 6 columnas, 42,82 MiB). |
# | **Salida** | `imagePoses.parquet`, `dataset_procesado/` (Parquet con compresión Snappy, particionado por ejercicio), `dataset_procesado.zip`, `resumen_ejercicios.csv` y la figura de distribución. |
# | **Flujo** | CSV crudo → Parquet → limpieza y análisis con Spark → Parquet particionado → ZIP para los notebooks 02 (Supabase y modelo) y 03 (Databricks). |
#
# **Ejecución.** En Colab: *Entorno de ejecución → Ejecutar todo*. Si no se desea depender de Kaggle, `imagePoses.csv` puede dejarse en `proyecto_big_data/raw/` antes de iniciar. En local se requiere Python 3.12, JDK 17 y `JAVA_HOME`.
#
# Las secciones usan la misma numeración del informe técnico (5.x, 6.x, 7.x). La última celda contrasta los resultados con los valores de referencia.
# %% [markdown]
# ## 5.2 Dependencias y entorno

# %%
# PySpark se fija en 3.5.3 (la versión del informe). El resto ya viene en Colab o se instala sin fijar versión.
# %pip install -q pyspark==3.5.3 pyarrow pillow kagglehub matplotlib

# %%
# --- 5.1 / 5.6 Configuración y estructura del proyecto ---
import os, sys, shutil, time, zipfile
from pathlib import Path
import pandas as pd

EN_COLAB = "google.colab" in sys.modules   # True si corre en Google Colab
# Carpeta de trabajo: /content en Colab; en local, ./proyecto_big_data junto al notebook
BASE = Path("/content/proyecto_big_data") if EN_COLAB else Path.cwd() / "proyecto_big_data"
RAW, PROCESSED, REPORTS = BASE / "raw", BASE / "processed", BASE / "reports"
for carpeta in (RAW, PROCESSED, REPORTS):
    carpeta.mkdir(parents=True, exist_ok=True)

MIB = 1024 ** 2                            # para mostrar tamaños en MiB
def mib(ruta):
    return ruta.stat().st_size / MIB

print("Entorno:", "Colab" if EN_COLAB else "local", "| Python", sys.version.split()[0], "| pandas", pd.__version__)
print("Carpeta de trabajo:", BASE)

# %%
# --- 5.2 Sesión de Spark (modo local: usa los núcleos de esta máquina, no es un clúster) ---
import pyspark
from pyspark.sql import SparkSession, functions as F

# Spark necesita Java 8/11/17. Colab ya lo trae; en local debe existir JAVA_HOME o `java` en el PATH.
if not (shutil.which("java") or os.environ.get("JAVA_HOME")):
    raise EnvironmentError("No se encontró Java. Instala JDK 17 y define JAVA_HOME.")
# Los procesos de Spark deben usar el mismo Python que este notebook (necesario en Windows)
os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (SparkSession.builder.appName("Evaluacion2_BigData").master("local[*]")
         .config("spark.sql.shuffle.partitions", "4")   # el informe (5.2) declara 4 particiones de intercambio
         .config("spark.driver.memory", "2g")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print("PySpark", pyspark.__version__, "| Spark", spark.version, "|", spark.sparkContext.master)

# %% [markdown]
# ## 6.1–6.3 Obtención, carga y exploración inicial

# %%
CSV_RAW = RAW / "imagePoses.csv"           # copia del dataset original, sin modificar

if not CSV_RAW.exists():
    # Opción A: descarga automática desde Kaggle (puede pedir credenciales)
    import kagglehub
    carpeta = Path(kagglehub.dataset_download("asher213/exercise-images-with-pose-estimation"))
    shutil.copy2(next(carpeta.rglob("imagePoses.csv")), CSV_RAW)
    # Opción B: subir imagePoses.csv a la carpeta raw/ y volver a ejecutar esta celda

print(f"CSV crudo: {CSV_RAW}  ({mib(CSV_RAW):.2f} MiB)")
df = pd.read_csv(CSV_RAW)                  # carga con pandas (~43 MB)

# %%
# --- 6.3 Exploración inicial ---
print("Dimensiones:", df.shape)            # esperado: (3501, 6)
print("Columnas:", df.columns.tolist())
display(df.drop(columns="base64encoded").head())   # sin la imagen, que es un texto Base64 muy largo

# %% [markdown]
# ## 6.4–6.5 Valores nulos y duplicados

# %%
print("Tipos de datos:\n" + df.dtypes.to_string())
print("\nNulos por columna:\n" + df.isna().sum().to_string())     # esperado: 860 en poseLandmarks
print("\nFilas duplicadas exactas:", int(df.duplicated().sum()))   # esperado: 0

# %%
# Duplicados y dimensiones según el CONTENIDO de la imagen.
# Comparar filas no basta: la misma foto puede aparecer con otro nombre o con otra etiqueta.
import base64, hashlib, io
from PIL import Image

def abrir(texto):
    """Convierte el texto Base64 (con o sin el envoltorio b'...') en una imagen."""
    texto = texto.strip()
    if texto[:2] in ("b'", 'b"'):
        texto = texto[2:-1]
    return Image.open(io.BytesIO(base64.b64decode(texto)))

huellas, tamanos, errores = [], [], 0
for texto in df["base64encoded"]:
    try:
        img = abrir(texto).convert("RGB")
        tamanos.append(img.size)                                           # (ancho, alto) reales
        huellas.append(hashlib.sha256(str(img.size).encode() + img.tobytes()).hexdigest())  # huella del contenido
    except Exception:
        errores += 1
        tamanos.append((None, None)); huellas.append(None)

ancho_real = pd.Series([t[0] for t in tamanos]); alto_real = pd.Series([t[1] for t in tamanos])
invertidas = ((df["imageWidth"] == alto_real) & (df["imageHeight"] == ancho_real) & (df["imageWidth"] != df["imageHeight"])).sum()
print("Imágenes que no se pudieron abrir:", errores)                                      # referencia: 0
print("Imágenes distintas por contenido:", pd.Series(huellas).nunique(), "de", len(df))   # referencia: 3385 (116 repetidas)
print("Ancho/alto invertidos respecto de la imagen real:", int(invertidas),
      "| imágenes cuadradas:", int((ancho_real == alto_real).sum()))                      # referencia: 3218 y 283

# %% [markdown]
# ## 6.9 Carga con Spark (CSV)

# %%
# multiLine y escape: el CSV tiene textos largos y comillas dentro de los campos
df_csv = (spark.read.option("header", True).option("inferSchema", True)
          .option("multiLine", True).option("escape", '"').csv(str(CSV_RAW)))
print("Registros:", df_csv.count(), "| columnas:", len(df_csv.columns))
df_csv.printSchema()
df_csv.drop("base64encoded").show(5, truncate=40)

# Nulos por columna y duplicados exactos, ahora con Spark (deben coincidir con pandas)
df_csv.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df_csv.columns]).show()
print("Duplicados exactos:", df_csv.count() - df_csv.dropDuplicates().count())

# %% [markdown]
# ## 6.10 Almacenamiento en Parquet y validación

# %%
RUTA_PARQUET = PROCESSED / "imagePoses.parquet"
df.to_parquet(RUTA_PARQUET, engine="pyarrow", index=False)   # index=False: no guardar el índice como columna

# Validación: se relee el archivo y se compara con el DataFrame original
chequeo = pd.read_parquet(RUTA_PARQUET)
assert chequeo.shape == df.shape and chequeo.columns.tolist() == df.columns.tolist(), "El Parquet no coincide con el CSV"
print(f"Parquet: {RUTA_PARQUET}")
print(f"Tamaño: {mib(RUTA_PARQUET):.2f} MiB | filas: {len(chequeo)} | nulos en poseLandmarks: {int(chequeo['poseLandmarks'].isna().sum())}")

# %% [markdown]
# ## 6.12 CSV vs Parquet: tamaño y tiempo de consulta

# %%
print(f"Tamaño  CSV: {mib(CSV_RAW):.2f} MiB | Parquet: {mib(RUTA_PARQUET):.2f} MiB "
      f"| variación: {(mib(RUTA_PARQUET) - mib(CSV_RAW)) / mib(CSV_RAW) * 100:+.2f} %")   # referencia: -8.33 %

# Tiempo: la misma consulta (imágenes por ejercicio) en ambos formatos; cada medición lee el archivo desde cero
def consulta_csv():
    return (spark.read.option("header", True).option("multiLine", True).option("escape", '"')
            .csv(str(CSV_RAW)).groupBy("excercise").count().collect())

def consulta_parquet():
    return spark.read.parquet(str(RUTA_PARQUET)).groupBy("excercise").count().collect()

def mediana_s(consulta, repeticiones=3):
    consulta()                                   # calentamiento: la 1.ª ejecución incluye la puesta en marcha de Spark
    tiempos = []
    for _ in range(repeticiones):
        t0 = time.perf_counter(); consulta(); tiempos.append(time.perf_counter() - t0)
    return sorted(tiempos)[len(tiempos) // 2]

t_csv, t_parquet = mediana_s(consulta_csv), mediana_s(consulta_parquet)
print(f"Tiempo (mediana de 3): CSV {t_csv:.2f} s | Parquet {t_parquet:.2f} s")
# Es una sola máquina y un archivo pequeño: sirve de comparación, no de prueba de escalabilidad.

# %% [markdown]
# ## 6.6–6.8 Preparación de los datos (Spark)

# %%
df_spark = spark.read.parquet(str(RUTA_PARQUET))   # desde aquí se trabaja con el Parquet validado

# 6.8 Normalizar la etiqueta: quitar espacios y pasar a minúsculas ("excercise" es el nombre original de la columna)
df_spark = df_spark.withColumn("label", F.lower(F.trim(F.col("excercise"))))

# 6.6 Registros inválidos: sin etiqueta o sin dimensiones. NO se descartan los que no tienen pose:
# el análisis descriptivo los conserva y el notebook 02 los filtra para el modelo.
valido = (F.col("label").isNotNull() & (F.col("label") != "")
          & F.col("imageWidth").isNotNull() & F.col("imageHeight").isNotNull())
df_base = df_spark.filter(valido).cache()
print("Registros:", df_spark.count(), "| válidos:", df_base.count(), "| descartados:", df_spark.count() - df_base.count())

# %% [markdown]
# ## 7.1–7.3 Análisis descriptivo (Spark)

# %%
# 7.1 Estadísticas de las dimensiones (6.7: se convierten a double). Nota: en la fuente ancho y alto vienen
# invertidos (ver el diagnóstico de 6.5), así que "imageWidth" es en realidad el alto. Aquí se reportan tal como están.
df_dim = (df_base.withColumn("imageWidth", F.col("imageWidth").cast("double"))
                 .withColumn("imageHeight", F.col("imageHeight").cast("double")))
df_dim.select("imageWidth", "imageHeight").describe().show()

# %%
# 7.2 Distribución de ejercicios (cantidad y porcentaje)
total = df_base.count()
df_dist = (df_base.groupBy("label").agg(F.count("*").alias("cantidad"))
           .withColumn("porcentaje", F.round(F.col("cantidad") * 100 / total, 2))
           .orderBy(F.desc("cantidad"), "label"))   # "label" desempata para que el orden sea siempre el mismo
df_dist.show(truncate=False)

# 7.3 Dimensiones por ejercicio
(df_dim.groupBy("label")
       .agg(F.count("*").alias("cantidad"),
            F.round(F.avg("imageWidth"), 2).alias("ancho_prom"), F.round(F.avg("imageHeight"), 2).alias("alto_prom"),
            F.min("imageWidth").alias("ancho_min"), F.max("imageWidth").alias("ancho_max"),
            F.min("imageHeight").alias("alto_min"), F.max("imageHeight").alias("alto_max"))
       .orderBy(F.desc("cantidad"), "label").show(truncate=False))

# %% [markdown]
# ## 7.4 Consultas con Spark SQL

# %%
df_base.createOrReplaceTempView("ejercicios")   # se registra el DataFrame como vista para usar SQL

# a) ¿Qué porcentaje de las imágenes de cada ejercicio no trae poseLandmarks? (patrón de datos faltantes, 7.5)
spark.sql("""
    SELECT label,
           COUNT(*)                                                                  AS imagenes,
           SUM(CASE WHEN poseLandmarks IS NULL THEN 1 ELSE 0 END)                    AS sin_pose,
           ROUND(100.0 * AVG(CASE WHEN poseLandmarks IS NULL THEN 1 ELSE 0 END), 1)  AS pct_sin_pose
    FROM ejercicios
    GROUP BY label
    ORDER BY pct_sin_pose DESC
""").show(truncate=False)

# b) Participación de cada ejercicio (misma tabla de 7.2, ahora con una función de ventana)
spark.sql("""
    SELECT label, COUNT(*) AS cantidad,
           ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS porcentaje
    FROM ejercicios
    GROUP BY label
    ORDER BY cantidad DESC, label
""").show(truncate=False)

# %% [markdown]
# ## 7.6 Visualización

# %%
import matplotlib.pyplot as plt

# Se recoge solo el resumen (9 filas) con collect(); no se pasa el dataset completo a pandas
filas = df_dist.collect()
etiquetas = [f["label"] for f in filas]
cantidades = [int(f["cantidad"]) for f in filas]
porcentajes = [float(f["porcentaje"]) for f in filas]

fig, ejes = plt.subplots(1, 2, figsize=(15, 5))
for eje, valores, titulo, formato in ((ejes[0], cantidades, "Registros por ejercicio", "{}"),
                                      (ejes[1], porcentajes, "Participación por ejercicio (%)", "{:.2f}%")):
    barras = eje.bar(etiquetas, valores)
    eje.set_title(titulo)
    eje.margins(y=0.12)                          # espacio arriba para que no se corten las etiquetas
    eje.bar_label(barras, labels=[formato.format(v) for v in valores], padding=2)
    plt.setp(eje.get_xticklabels(), rotation=45, ha="right")
plt.tight_layout()

FIGURAS = REPORTS / "figuras"; FIGURAS.mkdir(exist_ok=True)
plt.savefig(FIGURAS / "distribucion_ejercicios.png", dpi=150)   # queda guardada para el informe
plt.show()

# %% [markdown]
# ## 6.11 Parquet particionado por ejercicio (compresión Snappy)

# %%
import pyarrow as pa, pyarrow.parquet as pq, pyarrow.dataset as ds

RUTA_PROCESADOS = PROCESSED / "dataset_procesado"

df_proc = pd.read_parquet(RUTA_PARQUET)
df_proc["label"] = df_proc["excercise"].astype("string").str.strip().str.lower()   # misma normalización que en Spark
df_proc = df_proc[df_proc["label"].notna() & (df_proc["label"] != "")]             # solo se descartan etiquetas inválidas

# Se recrea la carpeta de salida para no mezclar ejecuciones distintas
if RUTA_PROCESADOS.exists():
    shutil.rmtree(RUTA_PROCESADOS)

# Se escribe con PyArrow: una carpeta label=<ejercicio>/ por categoría. No se usa la escritura de Spark
# porque en Windows requiere los binarios de Hadoop (winutils).
pq.write_to_dataset(pa.Table.from_pandas(df_proc, preserve_index=False), root_path=str(RUTA_PROCESADOS),
                    partition_cols=["label"], compression="snappy")

archivos = sorted(RUTA_PROCESADOS.rglob("*.parquet"))
print(f"Archivos Parquet: {len(archivos)} | tamaño total: {sum(a.stat().st_size for a in archivos) / MIB:.2f} MiB")

# %%
# Validación: PyArrow (particiones "hive") y Spark deben recuperar todas las filas escritas
n_arrow = ds.dataset(str(RUTA_PROCESADOS), format="parquet", partitioning="hive").to_table().num_rows
# Spark lee archivo por archivo (rutas explícitas) porque en Windows no puede listar carpetas sin winutils;
# basePath le permite reconstruir la columna "label" a partir de los nombres de carpeta. En Colab también funciona.
df_part = spark.read.option("basePath", str(RUTA_PROCESADOS)).parquet(*[str(a) for a in archivos])
n_spark = df_part.count()
print("Filas escritas:", len(df_proc), "| PyArrow:", n_arrow, "| Spark:", n_spark)
assert len(df_proc) == n_arrow == n_spark, "La lectura no recupera todas las filas"
df_part.groupBy("label").count().orderBy(F.desc("count"), "label").show(truncate=False)

# %% [markdown]
# ## Transferencia a los notebooks 02 y 03: ZIP y resumen

# %%
RUTA_ZIP = PROCESSED / "dataset_procesado.zip"
with zipfile.ZipFile(RUTA_ZIP, "w", compression=zipfile.ZIP_DEFLATED) as z:
    for archivo in archivos:
        z.write(archivo, arcname=archivo.relative_to(RUTA_PROCESADOS).as_posix())   # as_posix: mantiene label=.../ dentro del ZIP
print(f"ZIP: {RUTA_ZIP} ({mib(RUTA_ZIP):.2f} MiB, {len(archivos)} archivos)")

# Resumen por ejercicio (9 filas) en CSV
resumen = df_part.groupBy("label").count().withColumnRenamed("count", "cantidad").orderBy("label").collect()
pd.DataFrame([(f["label"], f["cantidad"]) for f in resumen], columns=["label", "cantidad"]) \
  .to_csv(REPORTS / "resumen_ejercicios.csv", index=False, encoding="utf-8-sig")

# En Colab: descargar el ZIP al equipo local (es la entrada de Databricks y del notebook 02)
if EN_COLAB:
    from google.colab import files
    files.download(str(RUTA_ZIP))

# %% [markdown]
# ## Verificación de resultados

# %%
# (nombre, obtenido, valor de referencia, tolerancia)
chequeos = [
    ("Filas del CSV",                      len(df),                                     3501,  0),
    ("Columnas del CSV",                   df.shape[1],                                 6,     0),
    ("Nulos en poseLandmarks",             int(df["poseLandmarks"].isna().sum()),       860,   0),
    ("Duplicados exactos",                 int(df.duplicated().sum()),                  0,     0),
    ("Categorías de ejercicio",            df_dist.count(),                             9,     0),
    ("Tamaño CSV (MiB)",                   mib(CSV_RAW),                                42.82, 0.05),
    ("Tamaño Parquet inicial (MiB)",       mib(RUTA_PARQUET),                           39.25, 0.5),
    ("Archivos Parquet particionados",     len(archivos),                               9,     0),
    ("Tamaño Parquet particionado (MiB)",  sum(a.stat().st_size for a in archivos) / MIB, 39.11, 0.5),
    ("Tamaño ZIP (MiB)",                   mib(RUTA_ZIP),                               29.35, 0.5),
]
print(f"{'Paso':38}{'Obtenido':>10}{'Referencia':>12}")
todo_ok = True
for nombre, obtenido, esperado, tolerancia in chequeos:
    ok = abs(obtenido - esperado) <= tolerancia
    todo_ok &= ok
    print(f"{nombre:38}{obtenido:>10.2f}{esperado:>12}  {'✔' if ok else '✘'}")
print("\nTodos los resultados coinciden con los valores de referencia." if todo_ok else "\nHay diferencias: revisar las filas marcadas con ✘.")
