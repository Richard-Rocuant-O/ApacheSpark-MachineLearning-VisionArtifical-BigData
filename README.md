# Pipeline de Big Data para clasificar ejercicios físicos

Evaluación Sumativa 02 · Big Data (TI3025) · INACAP, sede Punta Arenas · **Opción C**: Google Colab + PySpark + Databricks + Supabase (PostgreSQL).

Autores: Richard Rocuant y Luis Huanel.

## Descripción

Pipeline *batch* que ingiere, valida, almacena, analiza y modela un dataset con campos de gran tamaño (imágenes codificadas en Base64) y estructuras anidadas (33 puntos corporales por imagen), usando solo servicios gratuitos:

- **Colab + PySpark 3.5.3** (modo local): ingesta, validación, análisis con DataFrames y Spark SQL, y modelos con Spark MLlib.
- **Parquet + Snappy**, particionado por ejercicio.
- **Supabase (PostgreSQL)**: tabla de resumen por ejercicio, con seguridad por filas (RLS).
- **Databricks Free Edition**: lectura y validación del dataset desde un *Volume* de Unity Catalog.

La versión en Markdown del informe está en [`docs/INFORME.md`](docs/INFORME.md) y se usó como borrador de trabajo. El documento oficial es el informe en PDF entregado; si hay diferencias, prevalece el PDF.

## Dataset

[*Exercise Images With Pose Estimation*](https://www.kaggle.com/datasets/asher213/exercise-images-with-pose-estimation) (Kaggle, `asher213`): `imagePoses.csv`, 3.501 registros y 9 ejercicios. **No está en este repositorio**: los notebooks 01 y 04 lo descargan con KaggleHub, sin credenciales.

## Estructura

```
apache-spark-ml/
├── notebooks/   notebooks 01 a 04 (sin salidas)
├── scripts/     versiones .py de los notebooks (formato percent)
├── docs/        informe, capturas, diagramas y carta Gantt
├── requirements.txt
└── .gitignore
```

## Orden de ejecución

| Notebook | Dónde se ejecuta | Qué necesita | Qué produce |
|---|---|---|---|
| `01_Ingesta_Limpieza_Analisis_Almacenamiento_Datos` | Google Colab | Nada: descarga el CSV | `dataset_procesado.zip` (Parquet particionado), estadísticas y gráficos |
| `02_Supabase_Random_Forest` | Google Colab | El ZIP del notebook 01 y el secreto `SUPABASE_DB_URL` | Tabla `resumen_ejercicios` en Supabase, modelo Random Forest y métricas |
| `03_Validacion_Databricks` | Databricks Free Edition | El ZIP subido al *Volume* `/Volumes/workspace/default/dataset` | Validación del dataset leído con Spark |
| `04_Depuracion_Validacion_Cruzada` | Google Colab | Nada: descarga el CSV | Dataset depurado y validación cruzada de 5 grupos |

El 04 es independiente de los otros tres. Cada notebook termina con una tabla **Obtenido / Referencia**: si todo coincide, imprime «Todos los resultados coinciden con los valores de referencia».

### En Google Colab

1. **Archivo → Subir cuaderno** y elegir el notebook.
2. Para el 02: en **Secretos** (ícono de la llave) crear `SUPABASE_DB_URL` con la URI del *Session pooler* de Supabase (**Connect → Session pooler → URI**), activar «Acceso desde el cuaderno» y subir `dataset_procesado.zip` cuando el notebook lo pida.
3. **Ejecutar todo**.

### En Databricks

Subir el ZIP a un *Volume* (Catalog → `workspace` → `default` → *Create volume* `dataset`), importar el notebook 03 y **Run all** con cómputo *serverless*. Si los nombres del catálogo, esquema o volumen son otros, se cambia la variable `VOLUMEN` en la primera celda de código.

### En un equipo local (opcional)

Requiere Python 3 y Java 8, 11 o 17:

```bash
pip install -r requirements.txt
```

## Resultados de referencia

Valores obtenidos al ejecutar los notebooks en Colab, Supabase y Databricks (17-10-2026):

| Concepto | Valor |
|---|---|
| Registros / ejercicios | 3.501 / 9 |
| Registros sin puntos corporales | 860 (24,56 %) |
| Imágenes repetidas según su contenido | 116 (3.385 distintas) |
| Registros con ancho y alto invertidos | 3.218 |
| Random Forest, partición 80/20 (2.145 entrenamiento, 496 prueba) | exactitud 0,6875, F1 ponderado 0,6855 |
| Validación cruzada de 5 grupos, dataset depurado (3.321 registros) | Random Forest 0,7443 ± 0,0259; regresión logística 0,8213 ± 0,0181; árbol de decisión 0,6037 ± 0,0171 |

## Seguridad

La URI de conexión a Supabase se guarda solo en los **Secretos de Colab**: no se escribe en el código ni se imprime, y la conexión exige SSL. Este repositorio no contiene datos, el ZIP ni credenciales.

## Limitaciones

- El dataset es pequeño (42,82 MiB, 3.501 registros) para los estándares de Big Data: el trabajo demuestra el diseño y el flujo, no el rendimiento con grandes volúmenes.
- Spark se ejecuta en modo local (2 núcleos en Colab): no hay reparto real entre nodos.
- La alta disponibilidad se presenta como diseño (`docs/INFORME.md`, sección 3.8); no se desplegó ni se probó.
