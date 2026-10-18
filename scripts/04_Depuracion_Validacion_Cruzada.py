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
# # Evaluación 2 · Big Data — Notebook 04: depuración del dataset y validación cruzada
#
# **Opción C (entorno híbrido):** Google Colab + PySpark · PySpark 3.5.3.
#
# | | |
# |---|---|
# | **Objetivo** | Depurar el dataset (imágenes repetidas, etiquetas contradictorias y dimensiones invertidas) y evaluar el clasificador de ejercicios con validación cruzada de 5 grupos (*folds*), para obtener una medida más fiable que una única partición 80/20. |
# | **Entrada** | `imagePoses.csv` (3.501 registros, 6 columnas, 42,82 MiB): el mismo archivo del notebook 01. Si no está en `raw/`, se descarga con KaggleHub. |
# | **Salida** | `dataset_depurado.parquet` (3.321 registros y la columna `validation_fold`), el detalle de lo apartado en `reports/depuracion/`, las métricas de la validación cruzada (`reports/validacion_cruzada.csv`) y la figura `reports/figuras/validacion_cruzada.png`. |
# | **Flujo** | CSV original → decodificación de imágenes y huellas → depuración → asignación de 5 grupos → Parquet → Spark → 132 características de pose → validación cruzada de tres algoritmos → comparación. |
#
# **Ejecución.** En Colab: *Entorno de ejecución → Ejecutar todo*. No requiere Supabase ni Databricks. En local se requiere Python 3.12, JDK 17 y `JAVA_HOME`.
#
# Las secciones usan la numeración del informe técnico (6.x, 8.x y 9.x). La última celda contrasta los resultados de la depuración con los valores de referencia.

# %% [markdown]
# ## 5.2 Dependencias y sesión de Spark

# %%
# PySpark fijado en 3.5.3. scikit-learn aporta la división estratificada en grupos. setuptools: PySpark 3.5 usa `distutils`,
# que Python 3.12 ya no incluye.
# %pip install -q pyspark==3.5.3 pyarrow pillow kagglehub matplotlib scikit-learn setuptools

# %%
import os, sys, shutil, statistics, base64, hashlib, io
from pathlib import Path
import pandas as pd
import pyspark
from PIL import Image
from sklearn.model_selection import StratifiedKFold
from pyspark.sql import SparkSession, functions as F

EN_COLAB = "google.colab" in sys.modules                                    # True si corre en Google Colab
BASE = Path("/content/proyecto_big_data") if EN_COLAB else Path.cwd() / "proyecto_big_data"
RAW, PROCESSED, REPORTS = BASE / "raw", BASE / "processed", BASE / "reports"
DEPURACION, FIGURAS = REPORTS / "depuracion", REPORTS / "figuras"
for carpeta in (RAW, PROCESSED, DEPURACION, FIGURAS):
    carpeta.mkdir(parents=True, exist_ok=True)
SEMILLA = 42

# Spark necesita Java 8/11/17. Colab ya lo trae; en local debe existir JAVA_HOME o `java` en el PATH.
if not (shutil.which("java") or os.environ.get("JAVA_HOME")):
    raise EnvironmentError("No se encontró Java. Instala JDK 17 y define JAVA_HOME.")
os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable   # mismo Python para los procesos de Spark

# 2 núcleos fijos (los del Colab gratuito): los resultados de Spark dependen de las particiones y estas de los núcleos.
spark = (SparkSession.builder.appName("Evaluacion2_BigData_04").master("local[2]")
         .config("spark.sql.shuffle.partitions", "4").config("spark.driver.memory", "2g").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print("PySpark", pyspark.__version__, "| Spark", spark.version, "|", spark.sparkContext.master)

# %% [markdown]
# ## 6.1 Obtención y carga del dataset

# %%
CSV_RAW = RAW / "imagePoses.csv"                   # copia del dataset original, sin modificar
if not CSV_RAW.exists():
    import kagglehub                               # descarga automática desde Kaggle (puede pedir credenciales)
    carpeta = Path(kagglehub.dataset_download("asher213/exercise-images-with-pose-estimation"))
    shutil.copy2(next(carpeta.rglob("imagePoses.csv")), CSV_RAW)

df = pd.read_csv(CSV_RAW)
print(f"CSV original: {CSV_RAW.stat().st_size / 1024 ** 2:.2f} MiB | registros: {len(df)} | columnas: {df.shape[1]}")   # esperado: 42.82 | 3501 | 6

# %% [markdown]
# ## 6.5–6.7 Depuración del dataset

# %%
# Etiquetas normalizadas: cada nombre original de la fuente se asigna a un nombre de ejercicio único.
MAPA = {"pullup": "Pull-up", "plank": "Plank", "bent over row": "Bent-over row", "shoulder press": "Shoulder press",
        "squat": "Squat", "lunges": "Lunges", "pushup": "Push-up", "chest dips": "Chest dips", "glute bridge": "Glute bridge"}
trabajo = df.copy()
trabajo.insert(0, "source_row", range(len(trabajo)))               # posición en el CSV original (trazabilidad)
trabajo["label"] = trabajo["excercise"].map(MAPA)
assert trabajo["label"].notna().all(), "Hay ejercicios sin nombre normalizado"

def abrir(texto):
    """Convierte el texto Base64 (con o sin el envoltorio b'...') en una imagen RGB."""
    texto = texto.strip()
    if texto[:2] in ("b'", 'b"'):
        texto = texto[2:-1]
    with Image.open(io.BytesIO(base64.b64decode(texto))) as imagen:
        return imagen.convert("RGB")

def huella(imagen):
    """SHA256 del tamaño y de los píxeles: dos imágenes idénticas dan la misma huella (no detecta imágenes parecidas)."""
    return hashlib.sha256(str(imagen.size).encode("ascii") + imagen.tobytes()).hexdigest()

huellas, anchos, altos, errores = [], [], [], []
for fila in trabajo.itertuples(index=False):
    try:
        imagen = abrir(fila.base64encoded)
        huellas.append(huella(imagen)); anchos.append(imagen.width); altos.append(imagen.height)
    except Exception as exc:
        huellas.append(None); anchos.append(None); altos.append(None); errores.append((fila.source_row, str(exc)))

trabajo["image_hash"] = huellas
validas = trabajo[trabajo["image_hash"].notna()].copy()           # las imágenes que se pudieron abrir

# Dimensiones: lo declarado en el CSV frente a lo real. En la fuente el ancho y el alto vienen invertidos.
n_dim_distintas = int(((trabajo["imageWidth"] != pd.Series(anchos)) | (trabajo["imageHeight"] != pd.Series(altos))).sum())
validas["imageWidth"] = pd.Series(anchos, index=trabajo.index)[validas.index].astype(int)    # se reemplazan por los valores reales
validas["imageHeight"] = pd.Series(altos, index=trabajo.index)[validas.index].astype(int)

# 1) Etiquetas contradictorias: la misma imagen con más de un ejercicio. No se elige una al azar: se apartan todas sus filas.
etiquetas_por_imagen = validas.groupby("image_hash")["label"].nunique()
hashes_conflicto = etiquetas_por_imagen[etiquetas_por_imagen > 1].index
conflictos = validas[validas["image_hash"].isin(hashes_conflicto)].copy()
consistentes = validas[~validas["image_hash"].isin(hashes_conflicto)].copy()

# 2) Copias exactas: se conserva la primera aparición de cada imagen. Se hace después de apartar los conflictos para no ocultarlos.
copias = consistentes[consistentes["image_hash"].duplicated(keep="first")].copy()
limpio = consistentes.drop_duplicates("image_hash", keep="first").reset_index(drop=True)

# Comprobaciones: las cuentas deben cuadrar y cada imagen debe quedar una sola vez
assert len(df) == len(errores) + len(conflictos) + len(copias) + len(limpio)
assert limpio["image_hash"].is_unique and limpio["label"].notna().all()

# Lo apartado se guarda para poder revisarlo (sin la imagen en Base64, que pesa mucho)
conflictos.drop(columns="base64encoded").to_csv(DEPURACION / "etiquetas_en_conflicto.csv", index=False)
copias.drop(columns="base64encoded").to_csv(DEPURACION / "copias_retiradas.csv", index=False)

print("Imágenes que no se pudieron abrir:", len(errores))                                       # esperado: 0
print("Imágenes distintas por contenido:", validas["image_hash"].nunique(), "de", len(df))      # esperado: 3385
print("Dimensiones declaradas distintas de las reales:", n_dim_distintas)                       # esperado: 3218
print("Imágenes con etiquetas contradictorias:", len(hashes_conflicto), "| filas apartadas:", len(conflictos))   # esperado: 64 | 141
print("Copias exactas retiradas:", len(copias))                                                 # esperado: 39
print("Registros originales:", len(df), "| registros depurados:", len(limpio))                  # esperado: 3501 | 3321
print("\nRegistros por ejercicio: original -> depurado")
print(pd.DataFrame({"original": df["excercise"].map(MAPA).value_counts(), "depurado": limpio["label"].value_counts()})
      .fillna(0).astype(int).sort_values("depurado", ascending=False).to_string())

# %% [markdown]
# ## 9.2 Asignación de los grupos (*folds*) y almacenamiento en Parquet

# %%
# Cinco grupos estratificados por ejercicio (cada grupo mantiene la proporción de cada clase) y mezclados con semilla fija.
assert limpio["label"].value_counts().min() >= 5
limpio["validation_fold"] = -1
filas_folds = []
for fold, (tr, va) in enumerate(StratifiedKFold(n_splits=5, shuffle=True, random_state=SEMILLA).split(limpio, limpio["label"]), 1):
    entrenamiento, validacion = limpio.iloc[tr], limpio.iloc[va]
    compartidas = set(entrenamiento["image_hash"]) & set(validacion["image_hash"])      # imágenes en ambos lados = fuga de datos
    assert not compartidas, f"Hay imágenes compartidas entre entrenamiento y prueba en el grupo {fold}"
    assert set(entrenamiento["label"]) == set(validacion["label"]) == set(MAPA.values())  # los 9 ejercicios en ambos lados
    limpio.loc[limpio.index[va], "validation_fold"] = fold
    filas_folds.append({"fold": fold, "entrenamiento": len(tr), "prueba": len(va), "imagenes_compartidas": len(compartidas)})
assert limpio["validation_fold"].between(1, 5).all()
pd.DataFrame(filas_folds).to_csv(DEPURACION / "resumen_folds.csv", index=False)
print(pd.DataFrame(filas_folds).to_string(index=False))

# Dataset depurado en Parquet. Se omite la imagen en Base64: el modelo usa solo la pose.
COLUMNAS = ["source_row", "imageName", "imageWidth", "imageHeight", "poseLandmarks", "excercise", "label", "image_hash", "validation_fold"]
RUTA_DEPURADO = PROCESSED / "dataset_depurado.parquet"
limpio[COLUMNAS].to_parquet(RUTA_DEPURADO, engine="pyarrow", index=False)
print(f"\nDataset depurado: {RUTA_DEPURADO.name} | {RUTA_DEPURADO.stat().st_size / 1024 ** 2:.2f} MiB | {len(limpio)} registros")

# %% [markdown]
# ## 8.4 Características de pose (Spark)

# %%
import setuptools                                   # restituye `distutils`, que PySpark 3.5 necesita en Python 3.12
from pyspark.ml.functions import array_to_vector

df_spark = spark.read.parquet(str(RUTA_DEPURADO))
n_total = df_spark.count()
n_con_pose = df_spark.filter(F.col("poseLandmarks").isNotNull()).count()
print("Registros leídos con Spark:", n_total, "| con pose:", n_con_pose)                               # esperado: 3321 | 2508

# Los números de `poseLandmarks` se extraen con una expresión regular de Spark, en el orden x, y, z, visibility de cada punto
# (33 puntos x 4 valores = 132). Solo se usan los registros con pose.
NUMERO = "-?(?:[0-9]+(?:[.][0-9]*)?|[.][0-9]+)(?:[eE][+-]?[0-9]+)?"
df_modelo = (df_spark.filter(F.col("poseLandmarks").isNotNull())
                     .withColumn("features_array",
                                 F.expr("transform(regexp_extract_all(poseLandmarks, '(?:x|y|z|visibility): *(" + NUMERO + ")', 1),"
                                        " v -> cast(v as double))"))
                     .filter(F.size("features_array") == 132)
                     .withColumn("features", array_to_vector("features_array"))
                     .select("image_hash", "label", F.col("validation_fold").alias("fold"), "features").cache())
n_modelo = df_modelo.count()
print("Registros para el modelo (con 132 valores de pose):", n_modelo)                                 # esperado: 2508
print("\nRegistros con pose por grupo:")
df_modelo.groupBy("fold").count().orderBy("fold").show()                                               # esperado: 500, 511, 487, 500, 510
print("Registros con pose por ejercicio:")
df_modelo.groupBy("label").count().orderBy(F.desc("count"), "label").show(truncate=False)

# %% [markdown]
# ## 9.2 Evaluación: partición única y validación cruzada

# %%
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.ml.classification import RandomForestClassifier, DecisionTreeClassifier, LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def algoritmos():
    """Tres clasificadores de pyspark.ml. El Random Forest tiene los mismos parámetros del modelo del notebook 02."""
    return {
        "Random Forest": RandomForestClassifier(featuresCol="features", labelCol="label_index", numTrees=50, maxDepth=10, seed=SEMILLA),
        "Árbol de decisión": DecisionTreeClassifier(featuresCol="features", labelCol="label_index", maxDepth=10, seed=SEMILLA),
        "Regresión logística": LogisticRegression(featuresCol="features", labelCol="label_index", maxIter=100),
    }

def entrenar(estimador, entrenamiento):
    """Pipeline: la etiqueta de texto se convierte en índice y luego se ajusta el clasificador (solo con datos de entrenamiento)."""
    return Pipeline(stages=[StringIndexer(inputCol="label", outputCol="label_index", handleInvalid="skip"), estimador]).fit(entrenamiento)

def metricas(predicciones):
    return {nombre: MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName=m).evaluate(predicciones)
            for nombre, m in (("accuracy", "accuracy"), ("f1", "f1"), ("precision", "weightedPrecision"), ("recall", "weightedRecall"))}

# a) Partición única 80/20 al azar, igual que en el notebook 02 pero sobre el dataset depurado (sin imágenes repetidas)
entrenamiento, prueba = df_modelo.randomSplit([0.8, 0.2], seed=SEMILLA)
holdout = metricas(entrenar(algoritmos()["Random Forest"], entrenamiento).transform(prueba))
print(f"Partición única 80/20 (Random Forest): entrenamiento {entrenamiento.count()} | prueba {prueba.count()} | "
      f"exactitud {holdout['accuracy']:.4f} | F1 {holdout['f1']:.4f}")

# %%
# b) Validación cruzada de 5 grupos con los grupos asignados arriba: en cada vuelta un grupo es la prueba y los otros cuatro
# el entrenamiento, y se repite hasta que cada imagen se haya usado una vez como prueba.
filas, oof = [], []                                    # métricas por vuelta; predicciones de prueba del Random Forest
for nombre, estimador in algoritmos().items():
    for k in range(1, 6):
        entrenamiento = df_modelo.filter(F.col("fold") != k)
        prueba = df_modelo.filter(F.col("fold") == k)
        modelo = entrenar(estimador, entrenamiento)
        pred = modelo.transform(prueba)
        filas.append({"algoritmo": nombre, "fold": k, "entrenamiento": entrenamiento.count(), "prueba": prueba.count(), **metricas(pred)})
        if nombre == "Random Forest":
            nombres = IndexToString(inputCol="prediction", outputCol="predicho", labels=modelo.stages[0].labels).transform(pred)
            oof += [(f["label"], f["predicho"]) for f in nombres.select("label", "predicho").collect()]
    print("listo:", nombre)

resultados = pd.DataFrame(filas)
resultados.to_csv(REPORTS / "validacion_cruzada.csv", index=False)
print("\nResultados por vuelta (fold):")
print(resultados.round(4).to_string(index=False))

# %% [markdown]
# ## 9.3 Métricas: media y desviación estándar

# %%
# Resumen por algoritmo: media y desviación estándar de las 5 vueltas
resumen = (resultados.groupby("algoritmo")[["accuracy", "f1", "precision", "recall"]].agg(["mean", "std"]).round(4))
resumen.columns = [f"{m}_{e}" for m, e in resumen.columns]
resumen = resumen.sort_values("accuracy_mean", ascending=False)
print(resumen.to_string())

rf = resultados[resultados["algoritmo"] == "Random Forest"]
print(f"\nRandom Forest: exactitud {rf['accuracy'].mean():.4f} ± {rf['accuracy'].std():.4f} "
      f"(mín. {rf['accuracy'].min():.4f}, máx. {rf['accuracy'].max():.4f}) | F1 {rf['f1'].mean():.4f} ± {rf['f1'].std():.4f}")

# %% [markdown]
# ## 9.4 Visualización

# %%
import matplotlib.pyplot as plt

orden = resumen.index.tolist()
fig, eje = plt.subplots(figsize=(8, 5))
for i, nombre in enumerate(orden):
    valores = resultados.loc[resultados["algoritmo"] == nombre, "accuracy"]
    eje.scatter([i] * len(valores), valores, color="tab:blue", alpha=0.6, label="Vuelta (fold)" if i == 0 else None)
    eje.errorbar(i, valores.mean(), yerr=valores.std(), fmt="_", color="tab:red", markersize=30, capsize=6,
                 label="Media ± desv. estándar" if i == 0 else None)
eje.axhline(0.6875, color="gray", linestyle="--", label="Partición única original: 0,6875")
eje.set_xticks(range(len(orden)))
eje.set_xticklabels(orden)
eje.set_ylabel("Exactitud")
eje.set_title("Validación cruzada de 5 grupos (dataset depurado)")
eje.legend(loc="lower left")
plt.tight_layout()
plt.savefig(FIGURAS / "validacion_cruzada.png", dpi=150)
plt.show()

# %% [markdown]
# ## 9.5 Matriz de confusión global (Random Forest)

# %%
# Cada imagen aparece una vez como prueba: la matriz usa las 2.508 predicciones fuera de muestra de las 5 vueltas.
oof_df = pd.DataFrame(oof, columns=["real", "predicho"])
matriz = pd.crosstab(oof_df["real"], oof_df["predicho"])
matriz = matriz.reindex(index=sorted(matriz.index), columns=sorted(matriz.columns), fill_value=0)
print("Filas = ejercicio real; columnas = ejercicio predicho (los aciertos están en la diagonal)")
print(matriz.to_string())

por_clase = pd.DataFrame({
    "soporte": matriz.sum(axis=1),
    "recall": (pd.Series({c: matriz.loc[c, c] for c in matriz.index}) / matriz.sum(axis=1)).round(3),
    "precision": (pd.Series({c: matriz.loc[c, c] for c in matriz.index}) / matriz.sum(axis=0)).round(3),
}).sort_values("recall")
print("\nRecall y precisión por ejercicio:")
print(por_clase.to_string())
print("\nExactitud global de las predicciones fuera de muestra:", round(sum(matriz.loc[c, c] for c in matriz.index) / matriz.values.sum(), 4))

# %% [markdown]
# ## 9.7 Comparación entre evaluaciones

# %%
comparacion = pd.DataFrame([
    ("Partición única 80/20 (notebook 02)", "3.501 originales (2.641 con pose)", "0,6875 (referencia)"),
    ("Partición única 80/20 (mismo método)", f"{n_total:,} depurados ({n_modelo:,} con pose)".replace(",", "."), f"{holdout['accuracy']:.4f}"),
    ("Validación cruzada de 5 grupos", f"{n_total:,} depurados ({n_modelo:,} con pose)".replace(",", "."),
     f"{rf['accuracy'].mean():.4f} ± {rf['accuracy'].std():.4f}"),
], columns=["Evaluación (Random Forest)", "Datos", "Exactitud"])
print(comparacion.to_string(index=False))
print("\nLos grupos no comparten imágenes idénticas (huella única por imagen). No hay identificador de persona: la evaluación\n"
      "no prueba la generalización a personas nuevas.")

# %% [markdown]
# ## Verificación de resultados

# %%
# (nombre, obtenido, valor de referencia)
por_grupo = {f["fold"]: f["count"] for f in df_spark.groupBy("validation_fold").count().withColumnRenamed("validation_fold", "fold").collect()}
por_grupo_pose = {f["fold"]: f["count"] for f in df_modelo.groupBy("fold").count().collect()}
chequeos = [
    ("Registros del CSV original",         len(df),                                              3501),
    ("Imágenes que no se pudieron abrir",  len(errores),                                         0),
    ("Imágenes distintas por contenido",   int(validas["image_hash"].nunique()),                 3385),
    ("Dimensiones declaradas distintas",   n_dim_distintas,                                      3218),
    ("Imágenes con etiquetas contradictorias", len(hashes_conflicto),                            64),
    ("Filas apartadas por contradicción",  len(conflictos),                                      141),
    ("Copias exactas retiradas",           len(copias),                                          39),
    ("Registros depurados",                len(limpio),                                          3321),
    ("Registros con pose",                 n_con_pose,                                           2508),
    ("Registros por grupo (1 a 5)",        "-".join(str(por_grupo[k]) for k in range(1, 6)),      "665-664-664-664-664"),
    ("Con pose por grupo (1 a 5)",         "-".join(str(por_grupo_pose[k]) for k in range(1, 6)), "500-511-487-500-510"),
    ("Imágenes compartidas entre grupos",  sum(f["imagenes_compartidas"] for f in filas_folds),   0),
    ("Vueltas de validación cruzada",      int(resultados["fold"].nunique()),                    5),
]
print(f"{'Paso':42}{'Obtenido':>22}{'Referencia':>22}")
todo_ok = True
for nombre, obtenido, esperado in chequeos:
    ok = obtenido == esperado
    todo_ok &= ok
    print(f"{nombre:42}{str(obtenido):>22}{str(esperado):>22}  {'✔' if ok else '✘'}")
print("\nTodos los resultados coinciden con los valores de referencia." if todo_ok else "\nHay diferencias: revisar las filas marcadas con ✘.")
