# Big Data en la nube para la clasificación de ejercicios físicos

## Pipeline de ingesta, procesamiento, almacenamiento y modelamiento con PySpark, Supabase y Databricks (Opción C)

| | |
|---|---|
| **Integrantes** | Richard Rocuant y Luis Huanel |
| **Fecha de entrega** | 19 de octubre de 2026 |

---

## 1. Introducción
<!-- Responsable: Richard -->

### 1.1. Contexto del proyecto

El crecimiento de los datos generados por dispositivos, sensores y aplicaciones ha impulsado el uso de arquitecturas de procesamiento distribuido y de servicios en la nube para almacenarlos, transformarlos y analizarlos. En paralelo, la estimación de pose mediante visión artificial permite describir la postura del cuerpo humano con coordenadas, lo que abre aplicaciones en educación física, rehabilitación y análisis del movimiento.

Este trabajo corresponde a la Evaluación Sumativa 02 de la asignatura Big Data y consiste en diseñar e implementar un pipeline de Big Data en la nube utilizando servicios en sus niveles gratuitos (*Free Tier*). Se emplea el dataset público *Exercise Images With Pose Estimation* (Kaggle), con 3.501 imágenes de nueve ejercicios físicos y los puntos corporales estimados para cada una, y se desarrolla bajo la **Opción C** de la guía: un entorno híbrido formado por Google Colab, PySpark, Databricks y Supabase (PostgreSQL).

### 1.2. Problemática a resolver

El dataset elegido guarda en un solo CSV (3.501 registros, 42,82 MiB) la fotografía de cada ejercicio y los puntos corporales estimados para ella. Trabajarlo bien exige resolver varias cosas a la vez, que se resumen en una pregunta:

> **¿Cómo ingerir, validar, almacenar, analizar y modelar un dataset con campos de gran tamaño (imágenes codificadas como texto) y estructuras anidadas (coordenadas corporales) usando únicamente servicios gratuitos, de forma reproducible y con un diseño que pueda escalar?**

**Qué hace difícil la pregunta**

1. **Campos de gran tamaño.** Cada registro trae la imagen completa codificada en Base64, es decir, como un texto muy largo. Un CSV no tiene tipos ni compresión y obliga a leer todo el archivo aunque solo se necesite una columna, por lo que conviene un formato columnar y comprimido (Parquet) y particionado.
2. **Estructura anidada guardada como texto.** El campo `poseLandmarks` contiene 33 puntos del cuerpo, cada uno con `x`, `y`, `z` y `visibility`, dentro de un único texto. Para entrenar un modelo hay que extraer esos 132 números (33 × 4) y convertirlos en un vector de características.
3. **Solo servicios gratuitos.** Los niveles gratuitos tienen límites de cómputo (por ejemplo, 2 núcleos en Colab) y no garantizan alta disponibilidad. Por eso los resultados se guardan fuera de la sesión de cómputo y el trabajo se reparte entre Colab, Databricks y Supabase.
4. **Reproducible y escalable.** Otra persona debe poder repetir el pipeline desde cero y obtener los mismos resultados (semillas fijas y tablas de verificación en los notebooks), y el código debe poder pasar de una máquina a un clúster sin reescribirse (PySpark y Parquet particionado). El volumen real es pequeño para Big Data, así que el trabajo demuestra el diseño y el flujo, no el rendimiento con grandes volúmenes (3.3 y 10.6).

**Ejemplo del punto 2: del texto al vector de características.** Se parte del primer registro con pose (un *pushup*), del que se muestran solo dos de sus 33 puntos.

*Antes:* un único texto en la columna `poseLandmarks`.

```text
['x: 0.7909860014915466,y: 0.2797396183013916,z: -0.7337014675140381,visibility: 0.9999721050262451,',
 'x: 0.786137580871582,y: 0.24491778016090393,z: -0.699945330619812,visibility: 0.999929666519165,',
 ... (31 puntos más) ...]
```

*Después:* cada registro pasa a ser una fila de 132 columnas numéricas, en el orden `x`, `y`, `z`, `visibility` de cada punto. Se muestran tres registros de ejercicios distintos y solo las primeras 8 y las últimas 4 columnas; `v` es `visibility` y el número que sigue a cada letra es el del punto (0 a 32). Valores redondeados a dos decimales.

| Imagen | Ejercicio | x0 | y0 | z0 | v0 | x1 | y1 | z1 | v1 | … | x32 | y32 | z32 | v32 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `full-body-person-doing-pushup0.jpeg` | pushup | 0,79 | 0,28 | −0,73 | 1,00 | 0,79 | 0,24 | −0,70 | 1,00 | … | 0,10 | 0,70 | 0,61 | 0,99 |
| `full-body-person-doing-squat0.jpeg` | squat | 0,52 | 0,21 | −0,42 | 0,87 | 0,53 | 0,18 | −0,40 | 0,90 | … | 0,40 | 0,95 | −0,29 | 0,28 |
| `full-body-person-doing-plank0.jpeg` | plank | −0,65 | 1,25 | −1,24 | 0,82 | −0,63 | 1,15 | −1,25 | 0,83 | … | −0,38 | 2,93 | 0,21 | 0,23 |

En la tercera fila hay valores de `x` negativos y de `y` mayores que 1: son puntos que el modelo ubica fuera del encuadre (8.3).

| Posición en la fila | Qué contiene | Valor en el primer registro (pushup) |
|---|---|---|
| 0, 1, 2, 3 | Punto 0 (nariz): `x`, `y`, `z`, `visibility` | 0,79; 0,28; −0,73; 1,00 |
| 4, 5, 6, 7 | Punto 1 (ojo izquierdo, interno): `x`, `y`, `z`, `visibility` | 0,79; 0,24; −0,70; 1,00 |
| … | … | … |
| 128, 129, 130, 131 | Punto 32 (punta del pie derecho): `x`, `y`, `z`, `visibility` | 0,10; 0,70; 0,61; 0,99 |

**Problemas de calidad que el pipeline debe detectar**

| Problema | Cantidad | Qué significa | Cómo se aborda |
|---|---|---|---|
| Registros sin puntos corporales | 860 de 3.501 (24,56 %) | `poseLandmarks` viene vacío: no hay características para el modelo | Se mantienen en el análisis descriptivo y se excluyen del modelo, que usa 2.641 registros (6.6) |
| Imágenes repetidas según su contenido | 116 (hay 3.385 imágenes distintas) | La misma imagen aparece más de una vez aunque cambie el nombre o la etiqueta, y no se ve comparando filas; podría caer a la vez en el entrenamiento y en la prueba | Se detecta con la huella SHA256 de los píxeles. Los notebooks 01 a 03 la detectan y documentan; el 04 retira las copias exactas (6.5 y 9.2) |
| Ancho y alto declarados de forma invertida | 3.218 registros | Las dimensiones del CSV no coinciden con las de la imagen real (por ejemplo, declarada 183 × 275 y real 275 × 183) | Se detecta decodificando cada imagen. Los notebooks 01 a 03 lo documentan; el 04 usa las dimensiones reales (6.7) |

Al depurar, el notebook 04 encontró además 141 registros (64 imágenes) cuyo contenido aparece con ejercicios distintos, es decir, con etiquetas contradictorias (9.2).

### 1.3. Objetivo general

Diseñar e implementar un pipeline de Big Data en la nube, bajo la Opción C, que permita ingerir, procesar, almacenar y analizar el dataset de ejercicios físicos, y entrenar y evaluar un modelo predictivo que clasifique el ejercicio a partir de los puntos corporales estimados.

### 1.4. Objetivos específicos

1. Diseñar una arquitectura por capas (ingesta, almacenamiento, procesamiento, análisis y modelamiento) y explicar sus mecanismos de alta disponibilidad, tolerancia a fallos, escalabilidad y redundancia.
2. Implementar la ingesta y el procesamiento *batch* con PySpark y pandas, incluida la validación de calidad (nulos, duplicados y tipos de datos).
3. Almacenar los datos en formato Parquet con compresión y particionamiento, y comparar su tamaño y tiempo de consulta con el CSV original.
4. Realizar un análisis descriptivo distribuido con DataFrames y Spark SQL.
5. Entrenar y evaluar un clasificador Random Forest con `pyspark.ml` e interpretar sus métricas.
6. Integrar el pipeline con una base de datos PostgreSQL en la nube (Supabase) y con un segundo entorno de Spark (Databricks), gestionando las credenciales de forma segura.
7. Planificar, distribuir y documentar el trabajo del equipo (carta Gantt, roles y bitácora) y publicar el código en un repositorio.

### 1.5. Alcance del proyecto

**Incluye:** un prototipo funcional (el pipeline ejecutable en los cuatro notebooks); procesamiento *batch* del dataset completo (3.501 registros, 42,82 MiB); almacenamiento en Parquet particionado; análisis descriptivo con Spark; un modelo Random Forest; integración con Supabase y Databricks; y la documentación del trabajo.

**No incluye:**
- Procesamiento en *streaming*, que se describe como extensión (3.7).
- Un clúster de varias máquinas: Spark se ejecuta en modo local.
- Una prueba de escalabilidad con volúmenes mayores.
- Una revisión visual de las etiquetas del dataset: se heredan de la fuente.
- Un uso clínico o de diagnóstico: el modelo es de carácter exploratorio.

---

## 2. Marco teórico
<!-- Responsable: Richard -->

### 2.1. Big Data

Se denomina *Big Data* al conjunto de datos cuyo volumen, velocidad de generación y variedad superan la capacidad de las herramientas tradicionales de almacenamiento y procesamiento (Laney, 2001). Su tratamiento exige técnicas y plataformas específicas, normalmente distribuidas, que permitan ingerir, almacenar, procesar y analizar los datos a un costo razonable.

En términos simples, hay Big Data cuando los datos ya no se pueden guardar o procesar en un tiempo razonable en un solo computador con las herramientas habituales (una planilla de cálculo o una base de datos tradicional en un servidor). Eso ocurre por alguno de estos tres motivos, que son las tres características que propuso Laney (2001) y que se amplían en 2.2:

- **Volumen:** son demasiados datos para una sola máquina (por ejemplo, millones de imágenes).
- **Velocidad:** los datos llegan de forma continua y deben procesarse a medida que llegan (por ejemplo, las lecturas de sensores o de cámaras).
- **Variedad:** vienen en formatos distintos (tablas, texto, imágenes, coordenadas) que no caben en una sola tabla tradicional.

La respuesta habitual es repartir los datos y el cálculo entre varios computadores (procesamiento distribuido, 2.3) y guardar los datos en formatos pensados para leerse por partes, como Parquet, en lugar de archivos que hay que leer completos.

**Relación con este trabajo.** Con 3.501 registros y 42,82 MiB, el dataset es pequeño y cabe sin problemas en una sola máquina, así que no es Big Data por su volumen. Se eligió por su variedad (texto largo, números, imágenes en Base64 y coordenadas anidadas), y se aplican las técnicas y herramientas que se usarían con volúmenes mayores para demostrar el diseño y el flujo, no el rendimiento a gran escala (3.3 y 10.6).

### 2.2. Características de Big Data

Las características se resumen en las «V» del Big Data: **volumen** (cantidad de datos), **velocidad** (ritmo de generación y de procesamiento), **variedad** (formatos estructurados, semiestructurados y no estructurados), **veracidad** (calidad y confiabilidad) y **valor** (utilidad que se extrae). En este trabajo la variedad es la más relevante: el dataset combina texto, números, imágenes codificadas en Base64 y coordenadas anidadas.

### 2.3. Procesamiento distribuido

El procesamiento distribuido divide los datos y el cómputo entre varios nodos que trabajan en paralelo y combinan sus resultados. El modelo MapReduce formalizó esta idea mediante funciones de transformación (*map*) y de agregación (*reduce*) sobre particiones de datos (Dean y Ghemawat, 2004). Sus costos principales son el movimiento de datos entre nodos (*shuffle*) y la coordinación de los fallos.

Un ejemplo simple: para contar cuántas imágenes hay de cada ejercicio, cada nodo cuenta las suyas en la porción de datos que le tocó (fase *map*) y después se suman los conteos parciales de cada ejercicio (fase *reduce*). Ningún nodo necesita ver todos los datos. Repartir el trabajo así tiene tres ventajas:

- **Cabe más:** los datos no tienen que entrar completos en la memoria o el disco de una sola máquina.
- **Es más rápido:** más nodos trabajando a la vez reducen el tiempo, lo que se llama escalabilidad horizontal.
- **Resiste fallos:** si un nodo se cae, solo se recalcula su parte en otro nodo, no todo el trabajo.

Los costos son los que menciona el párrafo anterior. El *shuffle* ocurre cuando hay que reorganizar los datos entre nodos, por ejemplo para juntar en un mismo nodo los conteos de un mismo ejercicio; se hace por la red y suele ser la etapa más lenta. La coordinación de los fallos es el trabajo adicional de detectar qué nodo falló y reasignar su parte.

### 2.4. Apache Spark y PySpark

Apache Spark es un motor unificado de procesamiento de datos a gran escala que ofrece APIs para transformaciones, consultas SQL, aprendizaje automático y *streaming* (Zaharia et al., 2016). Trabaja con evaluación perezosa y un plan de ejecución (DAG) que se optimiza antes de ejecutarse; su módulo de SQL y DataFrames se describe en Armbrust et al. (2015). **PySpark** es su interfaz en Python. Spark puede ejecutarse en un clúster o en **modo local**, que usa los núcleos de una sola máquina con la misma API y es lo que se emplea en este trabajo.

Para entender cómo trabaja Spark conviene conocer cuatro ideas:

- **DataFrame:** es una tabla con columnas y tipos, parecida a una tabla de SQL o de pandas, pero repartida en **particiones** que Spark procesa en paralelo.
- **Evaluación perezosa:** las transformaciones (filtrar, agrupar, calcular una columna) no se ejecutan al escribirlas. Spark arma un plan (DAG), lo optimiza y recién lo ejecuta cuando se pide un resultado, con una acción como `count()` o `show()`.
- **Una sola herramienta para varias tareas:** con la misma sesión se hacen transformaciones con DataFrames, consultas con Spark SQL y entrenamiento de modelos con **Spark MLlib**, su biblioteca de aprendizaje automático (Meng et al., 2016).
- **Modo local:** Spark corre en una sola máquina, con los núcleos disponibles. En este trabajo se usa en Google Colab, que ofrece 2 núcleos, y el código es el mismo que se ejecutaría en un clúster.

**Alcance en este trabajo.** Como Spark corre en modo local, no hay reparto real entre varios nodos y no se midió la ventaja de la distribución; lo que se aprovecha es el modelo de programación (DataFrames, Spark SQL y MLlib) y la posibilidad de escalar más adelante. Además, la decodificación de las imágenes y la depuración se hacen con pandas, no con Spark (6.9).

### 2.5. Computación en la nube

La computación en la nube es un modelo que permite acceder bajo demanda a recursos de cómputo, almacenamiento y servicios compartidos, con elasticidad y pago por uso (Mell y Grance, 2011). Se organiza en modelos de servicio (IaaS, PaaS y SaaS). Los proveedores ofrecen niveles gratuitos (*Free Tier*) con límites de capacidad y disponibilidad, que condicionan el diseño de este proyecto.

### 2.6. Data Lake y almacenamiento de datos

Un *Data Lake* es un repositorio que conserva los datos en su formato original o procesado, normalmente sobre almacenamiento de objetos, y los organiza en zonas (por ejemplo, datos crudos y datos procesados). Se diferencia de un *Data Warehouse*, que guarda datos estructurados y modelados para consulta analítica. En el proyecto, el CSV original se conserva en una zona `raw`, los datos transformados en Parquet en una zona `processed`, los *Volumes* de Databricks cumplen el papel de almacenamiento en la nube y Supabase (PostgreSQL) guarda un resumen relacional.

### 2.7. Procesamiento Batch y Streaming

El procesamiento *batch* trabaja sobre conjuntos de datos acotados y completos, con latencia alta pero con simplicidad y alto rendimiento. El *streaming* procesa flujos continuos de eventos con baja latencia. Spark permite ambos modelos con una API similar (*Structured Streaming*). Este proyecto utiliza *batch*, porque el dataset es estático y acotado.

### 2.8. Formato Parquet

Apache Parquet es un formato de archivo **columnar** de código abierto. Almacena los valores de cada columna de forma contigua, lo que permite leer solo las columnas necesarias, comprimir mejor (los valores de una columna son similares) y conservar el esquema dentro del archivo (Apache Parquet, s. f.). Admite códecs de compresión como Snappy, que prioriza la velocidad.

### 2.9. Particionamiento de datos

El particionamiento organiza los archivos en carpetas según los valores de una columna (por ejemplo `label=squat/`), de modo que una consulta que filtra por esa columna solo lee las particiones necesarias (*partition pruning*). Tiene un costo: demasiadas particiones generan muchos archivos pequeños y sobrecarga. Conviene elegir columnas de baja cardinalidad y alineadas con las consultas.

### 2.10. Machine Learning aplicado a Big Data

Spark MLlib ofrece algoritmos de aprendizaje automático distribuidos y un mecanismo de *pipelines* que encadena transformadores y estimadores (Meng et al., 2016). **Random Forest** combina muchos árboles de decisión entrenados sobre muestras aleatorias de los datos y características, y promedia sus votos, lo que reduce el sobreajuste de un árbol individual (Breiman, 2001). La evaluación usa métricas como exactitud, precisión, *recall* y F1, y técnicas como la validación cruzada para estimar el rendimiento con datos no vistos.

### 2.11. Alta disponibilidad

La alta disponibilidad es la capacidad de un sistema de mantenerse operativo con una interrupción mínima ante fallos. Se logra con **redundancia** de componentes, **conmutación automática** (*failover*), distribución en varias zonas de disponibilidad y monitoreo. Suele expresarse como un porcentaje de disponibilidad comprometido por el proveedor.

### 2.12. Tolerancia a fallos

La tolerancia a fallos es la capacidad de continuar o recuperarse correctamente cuando un componente falla. En Spark, los conjuntos de datos distribuidos conservan su **linaje** (la secuencia de transformaciones que los generó), de modo que una partición perdida puede recalcularse sin replicar todos los datos (Zaharia et al., 2012). En el almacenamiento, la tolerancia se logra replicando los datos y con respaldos.

### 2.13. Escalabilidad

La escalabilidad es la capacidad de un sistema para manejar más carga. La **vertical** agrega recursos a una máquina; la **horizontal** agrega máquinas. Spark está diseñado para escalar horizontalmente, y los servicios en la nube permiten hacerlo de forma elástica. En este trabajo se utiliza una sola máquina; la escalabilidad se analiza como diseño (3.9) y no como medición.

---

## 3. Diseño y arquitectura de la solución
<!-- Responsable: Richard -->

### 3.1. Descripción general de la solución

La solución es un pipeline *batch* organizado en cuatro capas: **ingesta**, **almacenamiento**, **procesamiento** y **análisis y modelamiento**. El archivo `imagePoses.csv` se ingiere y se valida en Google Colab con pandas y PySpark; PySpark realiza las transformaciones, el análisis y el modelamiento, y pandas con PyArrow escribe los archivos Parquet particionados; y se utiliza para dos propósitos: un análisis descriptivo y un clasificador Random Forest. Un resumen por ejercicio se guarda en una base PostgreSQL en la nube (Supabase), y el dataset procesado se valida en un segundo entorno de Spark (Databricks).

El trabajo se divide en cuatro notebooks: **01** (ingesta, limpieza, análisis y almacenamiento), **02** (integración con Supabase y modelo), **03** (validación en Databricks) y **04** (depuración del dataset y validación cruzada). El ZIP `dataset_procesado.zip` conecta los notebooks 01, 02 y 03; el 04 parte del mismo CSV original y genera su propio dataset depurado.

**Prototipo funcional.** El prototipo es el propio pipeline ejecutable: los cuatro notebooks se ejecutan de extremo a extremo en Google Colab, Supabase y Databricks, y cada uno verifica sus resultados con una tabla de comprobaciones (9.7 y capítulo 11). Se puede reproducir clonando el repositorio (11.6).

### 3.2. Dataset seleccionado

*Exercise Images With Pose Estimation* (Kaggle, usuario asher213). El archivo `imagePoses.csv` pesa 42,82 MiB y contiene **3.501 registros y 6 columnas**.

| Columna | Tipo | Contenido |
|---|---|---|
| `imageName` | texto | Nombre del archivo de imagen |
| `base64encoded` | texto | Imagen JPEG codificada en Base64 |
| `imageWidth`, `imageHeight` | entero | Dimensiones declaradas de la imagen |
| `poseLandmarks` | texto | Lista de 33 puntos corporales con `x`, `y`, `z` y `visibility` (nulo en 860 registros); un ejemplo y la lista de puntos están en 8.3 |
| `excercise` | texto | Ejercicio (nombre original de la columna, con esa ortografía) |

Nueve ejercicios: *bent over row*, *lunges*, *plank*, *pullup*, *shoulder press* y *squat* (400 registros cada uno), *pushup* (398), *chest dips* (389) y *glute bridge* (314).

### 3.3. Justificación de la selección del dataset

- **Es público y reproducible:** se descarga con KaggleHub sin credenciales (comprobado) y su contenido se verifica con una huella SHA256.
- **Es heterogéneo:** combina texto largo (Base64), números y estructuras anidadas, lo que da sentido a las decisiones de formato (Parquet) y de procesamiento con Spark.
- **Permite un problema supervisado claro:** clasificar el ejercicio a partir de la pose.
- **Tiene relevancia para el dominio:** análisis de ejercicios por visión artificial.

**Limitación declarada:** su volumen (43 MiB, 3.501 registros) es pequeño para los estándares de Big Data. El trabajo demuestra el diseño y el flujo, no el rendimiento con grandes volúmenes (ver 10.6 y 10.7).

### 3.4. Selección de tecnologías y herramientas

| Capa | Tecnología | Justificación |
|---|---|---|
| Ingesta | KaggleHub / carga manual | Descarga automática del dataset público |
| Procesamiento | PySpark 3.5.3 en Google Colab (transformaciones, SQL y características) y pandas/PyArrow (decodificación de imágenes, depuración y escritura de Parquet) | Spark es el motor distribuido; pandas se usa donde el trabajo es por imagen o por archivo. Colab gratuito con Java y Python listos |
| Formato | Parquet + Snappy | Columnar, comprimido y con esquema |
| Almacenamiento (lake) | Databricks Free Edition: *Volumes* de Unity Catalog | Almacenamiento en la nube, gobernado, accesible desde Spark |
| Almacenamiento (relacional) | Supabase (PostgreSQL) | Base gestionada en la nube con plan gratuito |
| Modelo | `pyspark.ml` (Random Forest) | Entrenamiento sobre el mismo motor de las transformaciones |
| Credenciales | Secretos de Colab | La URI de conexión no se escribe en el código |
| Código | GitHub | Control de versiones y entrega |

Se eligió la **Opción C** de la guía (Colab + PySpark + Databricks + Supabase) porque permite usar solo servicios gratuitos y cubre todas las capas. No se usaron servicios de AWS ni de Google Cloud. La guía menciona Databricks Community Edition; en este trabajo se usó **Databricks Free Edition** (cómputo *serverless*), la edición gratuita disponible al momento de la realización.

### 3.5. Arquitectura propuesta

| Capa | Componentes | Responsabilidad |
|---|---|---|
| **Ingesta** | KaggleHub, carpeta `raw/` | Obtener el CSV original y conservarlo sin modificar |
| **Almacenamiento** | Parquet en `processed/`; ZIP; *Volume* de Databricks; tabla en Supabase | Persistir los datos procesados y un resumen |
| **Procesamiento** | PySpark (transformaciones, SQL y características) y pandas/PyArrow (decodificación de imágenes, depuración y escritura de Parquet), en Colab | Validar, transformar y escribir Parquet particionado |
| **Análisis y modelamiento** | Spark SQL, DataFrames, MLlib | Estadísticas, consultas y clasificación |

### 3.6. Diagrama de arquitectura

![Figura 3.1. Arquitectura implementada](img/fig_3_1_arquitectura_implementada.png)

*Figura 3.1. Arquitectura implementada. Los recuadros son los tres servicios usados (Google Colab, Databricks y Supabase); las insignias indican la capa (1 ingesta, 2 almacenamiento, 3 procesamiento, 4 análisis y modelamiento) y el color, el tipo de componente.*

### 3.7. Flujo de datos y tipo de procesamiento (batch o streaming)

**Flujo de datos**

1. **Obtención:** el CSV se descarga con KaggleHub (o se carga a mano) y se conserva en `raw/` (notebook 01).
2. **Validación y exploración:** dimensiones, tipos, nulos y duplicados, con pandas y con Spark.
3. **Conversión a Parquet** y validación del archivo resultante.
4. **Preparación:** normalización de la etiqueta y control de registros inválidos.
5. **Análisis descriptivo** con DataFrames y Spark SQL, y generación de gráficos.
6. **Particionamiento** por ejercicio y empaquetado en `dataset_procesado.zip`.
7. **Notebook 02:** lectura del ZIP, resumen en Supabase, extracción de 132 características de pose, entrenamiento y evaluación.
8. **Notebook 03:** carga del ZIP al *Volume* de Databricks, lectura con Spark y validación.

**Tipo de procesamiento: batch.** El dataset es estático y acotado (3.501 registros) y no existe una fuente de eventos continua, por lo que el procesamiento por lotes es el adecuado. El *streaming* podría incorporarse con *Structured Streaming* leyendo nuevos archivos de una carpeta o un servicio de mensajería; **no se implementó**.

### 3.8. Alta disponibilidad, tolerancia a fallos y redundancia

Los servicios gratuitos usados **no garantizan alta disponibilidad**, y esta no se demostró experimentalmente. Por ello se distingue entre lo que el trabajo implementa y el diseño objetivo para un entorno de producción.

| Mecanismo | Implementado en este trabajo | Diseño objetivo en producción |
|---|---|---|
| **Persistencia fuera del cómputo** | Los resultados se guardan fuera de la sesión de Colab: ZIP descargado, *Volume* de Databricks y tabla en Supabase. Una sesión perdida no elimina lo ya persistido | Almacenamiento de objetos replicado entre zonas como fuente de verdad |
| **Recuperación de cálculos** | Los notebooks son **idempotentes**: pueden ejecutarse de nuevo sobre carpetas existentes (se verificó con el 01 y el 03) | Orquestador con reintentos automáticos |
| **Tolerancia a fallos de nodos** | Spark conserva el linaje de las transformaciones, pero se ejecuta en una sola máquina | Clúster Spark con varios nodos: un nodo caído se recalcula en otro |
| **Redundancia de datos** | La fuente original sigue disponible en Kaggle, y existen copias en Colab, Databricks y Supabase | Replicación entre zonas y copias de seguridad programadas |
| **Base de datos** | Supabase Free (PostgreSQL gestionado). El panel indica «No backups» | Réplica en otra zona y recuperación a un punto en el tiempo |

![Figura 3.2. Diseño objetivo con alta disponibilidad](img/fig_3_2_arquitectura_ha.png)

*Figura 3.2. Diseño objetivo con alta disponibilidad (no desplegado en este trabajo).*

**Cómo leer la figura.** Es un diseño, no algo desplegado. Las insignias indican la capa y el color, el tipo de componente. Los nodos Spark forman un grupo de autoescalado repartido en dos zonas de disponibilidad: si una zona falla, la otra sigue procesando y el *driver* reintenta las tareas perdidas (Spark las recalcula por el linaje de las transformaciones). El *Data Lake* y la base de datos se replican entre zonas; la réplica de PostgreSQL atiende consultas de solo lectura y puede reemplazar al primario ante una falla. La cola de mensajes de la ingesta permite reintentar sin perder datos, y el monitoreo y la gestión de secretos son servicios transversales.

### 3.9. Escalabilidad

- **Horizontal:** el código PySpark no cambia al pasar de modo local a un clúster; basta con apuntar a un gestor de clústeres (por ejemplo Databricks o Dataproc) y aumentar los nodos.
- **Datos:** el Parquet particionado permite leer solo las particiones necesarias; con volúmenes mayores convendría particionar por una columna de mayor utilidad (por ejemplo, fecha de ingesta).
- **Base de datos:** Supabase permite aumentar el tamaño del cómputo (hoy el plan gratuito usa el tamaño mínimo, *Nano*).
- **Límites actuales:** Spark usa 2 núcleos en una máquina y el dataset es pequeño. **No se midió** el comportamiento con volúmenes mayores.

### 3.10. Seguridad y control de acceso

- **Credenciales:** la URI de conexión a Supabase se guarda en los **Secretos de Colab** (`SUPABASE_DB_URL`) con acceso habilitado solo para el notebook que la usa; no se escribe en el código ni se imprime (solo se muestran host, puerto y usuario).
- **Conexión:** *Session pooler* de Supabase con `sslmode=require` (cifrada).
- **Base de datos:** la tabla tiene **seguridad por filas (RLS)** activada. Con RLS activado y sin políticas, la API pública de Supabase no devuelve filas de la tabla; el notebook accede por la conexión directa con el rol `postgres`, que no se ve afectada.
- **Contraseña:** durante el desarrollo se restableció la contraseña de la base de datos (ver bitácora, 4.6) y se usó una contraseña alfanumérica larga.
- **Databricks:** el *Volume* es administrado (*managed*) y pertenece a la cuenta del propietario. No se usan roles IAM de AWS ni de Google Cloud, porque no se emplean esos servicios.

---

## 4. Planificación y gestión del proyecto
<!-- Responsable: Richard -->

### 4.1. Organización del equipo

El equipo lo forman **Richard Rocuant** y **Luis Huanel**, estudiantes de Ingeniería en Informática (INACAP, sede Punta Arenas). El trabajo se coordinó con un repositorio de código, los notebooks ejecutados en Colab y Databricks, y un documento de trabajo con la bitácora de verificación.

### 4.2. Distribución de responsabilidades según las fortalezas de cada integrante

| Integrante | Fortalezas | Responsabilidades |
|---|---|---|
| **Richard Rocuant** | Desarrollo web (Laravel, React, TypeScript), PostgreSQL, Docker y certificación AWS Cloud Practitioner | Reproducción y verificación del pipeline; reescritura y prueba de los notebooks 01, 02 y 03; integración con Supabase y Databricks; bitácora; diseño de arquitectura, planificación e informe (capítulos 1 a 4, 9, 10, 12 y 13) |
| **Luis Huanel** | Implementación de pipelines con PySpark y configuración de servicios cloud (Supabase y Databricks), demostradas en el desarrollo original | Implementación original del pipeline (ingesta, análisis, Parquet, Supabase, modelo y Databricks); documentación de implementación y evidencias (capítulos 5 a 8 y 11) |

Ambos integrantes participaron en la parte técnica: Luis construyó el pipeline original y Richard lo reprodujo, lo verificó contra los resultados de referencia, corrigió errores y lo adaptó para ejecutarlo en Colab y Databricks.

### 4.3. Carta Gantt del proyecto

> **[PEGAR AQUÍ LA CARTA GANTT]** Abrir `informe/Carta_Gantt.xlsx`, copiar el rango A1:U15 y pegarlo en Word como imagen.

*Figura 4.1. Carta Gantt (archivo `Carta_Gantt.xlsx`). Las fechas de la etapa de implementación son aproximadas: se estimaron a partir de las fechas de los archivos del proyecto.*

### 4.4. Etapas y fechas de desarrollo

| Etapa | Fechas | Responsable | Estado |
|---|---|---|---|
| Lectura de la guía y definición de la Opción C | 05 a 06-10 | Ambos | Completada |
| Implementación del pipeline original | 14 a 16-10 | Luis | Completada |
| Reproducción y verificación de los notebooks | 16 a 17-10 | Richard | Completada |
| Depuración y validación cruzada (notebook 04) | 17-10 | Richard | Completada (ejecutada en local y en Colab) |
| Redacción del informe | 17 a 19-10 | Ambos | En curso |
| Repositorio de GitHub | 18 a 19-10 | Ambos | Pendiente |
| Entrega | 19-10, antes de las 18:00 | Ambos | Pendiente |

### 4.5. Hitos principales

| Hito | Estado |
|---|---|
| H1. Opción C definida y dataset seleccionado | Cumplido |
| H2. Pipeline operativo en la nube (Colab, Supabase y Databricks) | Cumplido |
| H3. Resultados reproducidos y verificados (tablas de verificación en ✔) | Cumplido |
| H4. Informe técnico completo | En curso |
| H5. Repositorio publicado y enlazado en el PDF | Pendiente |
| H6. Entrega puntual por el ambiente de aprendizaje | Pendiente |

### 4.6. Bitácora de avances e incidencias superadas

| Fecha | Etapa | Incidencia | Solución |
|---|---|---|---|
| 16-10 | Notebook 01 | `spark.read.parquet(<carpeta>)` falla en Windows con `NativeIO$Windows.access0`: Spark no puede listar carpetas sin los binarios de Hadoop (winutils) | Leer los archivos uno a uno con `basePath`. En Colab (Linux) no ocurre |
| 16-10 | Notebook 01 | Notebook original con rutas de Windows fijas | Rutas unificadas en una sola variable (`BASE`); ejecución en Colab con todo en ✔ |
| 17-10 | Notebook 02 | Las funciones de Python (UDF) de Spark fallan en el equipo local de Windows (`Python worker exited unexpectedly`), incluso una trivial | Extracción de las 132 características con `regexp_extract_all` de Spark; resultado idéntico al original en las 2.641 filas |
| 17-10 | Notebook 02 | `ModuleNotFoundError: distutils` en Python 3.12 | Instalar e importar `setuptools` |
| 17-10 | Notebook 02 | La división train/test dependía del número de núcleos (con 16: 2.130/511 y exactitud 0,7358) | Fijar Spark en 2 núcleos: la división queda 2.145/496 y exactitud 0,6875, en cualquier máquina |
| 17-10 | Notebook 02 | La ruta completa del notebook superaba los 260 caracteres de Windows | Nombre de archivo más corto |
| 17-10 | Supabase | `FATAL: tenant/user postgres.<ref> not found` | El secreto tenía el marcador `<ref>` en vez de la referencia real del proyecto |
| 17-10 | Supabase | `FATAL: password authentication failed` | Restablecer la contraseña de la base y actualizar el secreto |
| 17-10 | Notebook 02 | La verificación de la tabla se hacía dentro de la misma transacción | Releer con una conexión nueva, después del *commit* |
| 17-10 | Notebook 03 | `FileExistsError` al ejecutarlo por segunda vez: `dbutils.fs.rm` no eliminó la carpeta de destino | Reutilizar la carpeta y leer solo los archivos que trae el ZIP. Verificado en Databricks |
| 17-10 | Databricks | El catálogo por defecto de la Free Edition es `workspace` | Volumen `dataset` en `workspace.default` |

**Incidencias del desarrollo original** (según la documentación de Luis): permisos de escritura en Windows, limitaciones de escritura de Spark/Hadoop, error de conversión con `toPandas()` y formato no estándar de `poseLandmarks`. Se resolvieron con una ruta alternativa, escritura con PyArrow, recuperación del resumen con `collect()` y extracción numérica con expresiones regulares.

---

## 5. Implementación del entorno
<!-- Responsable: Luis -->

### 5.1. Configuración de Google Colab

El procesamiento se ejecuta en cuadernos de Google Colab con un entorno de ejecución de CPU (se observó Python 3.13). Spark se inicia en **modo local** (`local[*]` en el notebook 01 y `local[2]` en el 02) con 2 GB de memoria del *driver* y 4 particiones de intercambio (`spark.sql.shuffle.partitions`). Los archivos de la sesión residen en `/content` y **se pierden al terminar la sesión**, por lo que el ZIP se descarga al final del notebook 01.

**Código** (notebook 02, sección 5.2):

```python
# Se fijan 2 núcleos (los del Colab gratuito): randomSplit reparte las filas según las particiones de Spark, y estas
# dependen de los núcleos. Con 2 núcleos la división train/test sale igual en cualquier máquina.
spark = (SparkSession.builder.appName("Evaluacion2_BigData_02").master("local[2]")
         .config("spark.sql.shuffle.partitions", "4").config("spark.driver.memory", "2g").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print("PySpark", pyspark.__version__, "| Spark", spark.version, "|", spark.sparkContext.master)
```

*Qué hace:* Crea la sesión de Spark en modo local con 2 núcleos fijos (los del Colab gratuito), 4 particiones de intercambio y 2 GB de memoria del *driver*. Fijar los núcleos hace que la división aleatoria de los datos sea la misma en cualquier máquina.

### 5.2. Instalación y configuración de herramientas

| Notebook | Instalación |
|---|---|
| 01 | `pyspark==3.5.3 pyarrow pillow kagglehub matplotlib` |
| 02 | `pyspark==3.5.3 pyarrow "psycopg[binary]" setuptools` |
| 03 | Ninguna (Databricks trae Spark) |
| 04 | `pyspark==3.5.3 pyarrow pillow kagglehub matplotlib scikit-learn setuptools` |

PySpark se fija en la versión 3.5.3. Java ya viene instalado en Colab. Además, el pipeline se verificó en un entorno local aislado con Python 3.12, JDK 17 (Temurin), pandas 3.0.6, NumPy 2.5.4 y PyArrow 26.0.0.

### 5.3. Configuración de servicios Cloud

- **Supabase:** organización y proyecto en el plan **Free**, región *East US (North Virginia)*, estado *Healthy*, cómputo *Nano*. Conexión mediante el *Session pooler* (host `aws-0-us-east-1.pooler.supabase.com`, puerto 5432, usuario `postgres.<ref>`).
- **Databricks Free Edition:** *workspace* con cómputo *serverless*. Almacenamiento en un *Volume* administrado (`/Volumes/workspace/default/dataset`) del catálogo `workspace`.
- **Google Colab:** entorno de ejecución del ETL y del modelo.

### 5.4. Configuración de almacenamiento

| Ubicación | Contenido |
|---|---|
| `proyecto_big_data/raw/` | `imagePoses.csv` original, sin modificar |
| `proyecto_big_data/processed/` | `imagePoses.parquet`, `dataset_procesado/` (Parquet particionado) y `dataset_procesado.zip` |
| `proyecto_big_data/reports/` | `resumen_ejercicios.csv` y `figuras/` |
| Databricks *Volume* | `dataset_procesado.zip` y su extracción |
| Supabase | Tabla `resumen_ejercicios` |

**Código** (notebook 02, sección 11.3):

```python
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
```

*Qué hace:* Calcula con Spark el resumen por ejercicio, crea la tabla `resumen_ejercicios` en Supabase, activa la seguridad por filas, inserta o actualiza cada fila (`ON CONFLICT`), borra categorías antiguas y relee la tabla con una **conexión nueva** para comprobar lo que realmente quedó guardado.

### 5.5. Configuración de permisos y credenciales

La URI de Supabase se guarda como secreto de Colab con el nombre `SUPABASE_DB_URL`. El código la recupera con `userdata.get`, muestra solo host, puerto y usuario, y se conecta con `sslmode=require`. La tabla tiene la seguridad por filas (RLS) activada. En Databricks, el acceso al *Volume* pertenece al propietario del *workspace*. Ver 3.10.

**Código** (notebook 02, sección 11.2):

```python
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
```

*Qué hace:* La URI de conexión se obtiene del secreto de Colab `SUPABASE_DB_URL` (o de una variable de entorno en local). Solo se muestran host, puerto y usuario, **nunca la contraseña**, y la conexión exige SSL.

### 5.6. Estructura del proyecto

```
proyecto_big_data/
├── raw/        (CSV original)
├── processed/  (Parquet, particiones y ZIP)
└── reports/    (resumen y figuras)
```

La estructura del repositorio se describe en 11.6.

### 5.7. Uso y límites del Free Tier

| Servicio | Plan | Límites observados o relevantes |
|---|---|---|
| Google Colab | Gratuito | Sesiones temporales: el disco local se borra al terminar. Disco disponible observado: 86,10 GB |
| Supabase | Free | Cómputo *Nano*; sin respaldos (el panel muestra «No backups»). La tabla del proyecto tiene 9 filas |
| Databricks | Free Edition | Solo cómputo *serverless* (*Serverless Starter Warehouse 2XS*) |

No se registró consumo numérico de cuotas ni se incurrió en costos. Las cuotas exactas de cada plan se consultan en la documentación oficial de cada proveedor.

---

## 6. Ingesta y procesamiento de datos
<!-- Responsable: Luis -->

### 6.1. Obtención del dataset

El notebook 01 descarga el dataset con KaggleHub (`asher213/exercise-images-with-pose-estimation`) y copia `imagePoses.csv` a `raw/`. La descarga no requiere credenciales. El archivo descargado es idéntico al original (misma huella SHA256).

**Código** (notebook 01, sección 6.1):

```python
CSV_RAW = RAW / "imagePoses.csv"           # copia del dataset original, sin modificar

if not CSV_RAW.exists():
    # Opción A: descarga automática desde Kaggle (puede pedir credenciales)
    import kagglehub
    carpeta = Path(kagglehub.dataset_download("asher213/exercise-images-with-pose-estimation"))
    shutil.copy2(next(carpeta.rglob("imagePoses.csv")), CSV_RAW)
    # Opción B: subir imagePoses.csv a la carpeta raw/ y volver a ejecutar esta celda

print(f"CSV crudo: {CSV_RAW}  ({mib(CSV_RAW):.2f} MiB)")
df = pd.read_csv(CSV_RAW)                  # carga con pandas (~43 MB)
```

*Qué hace:* Si el CSV no está en `raw/`, se descarga desde Kaggle con KaggleHub (sin credenciales) y se copia sin modificarlo; luego se carga con pandas.

### 6.2. Carga de datos

Se carga primero con pandas (`read_csv`) y luego con Spark. En Spark se usan las opciones `header`, `inferSchema`, `multiLine` y `escape`, necesarias porque el CSV tiene textos largos y comillas dentro de los campos.

### 6.3. Exploración inicial

El dataset tiene **3.501 registros y 6 columnas** (`imageName`, `base64encoded`, `imageWidth`, `imageHeight`, `poseLandmarks` y `excercise`). Para la muestra se omite la columna `base64encoded`, que es un texto muy largo.

### 6.4. Identificación de valores nulos

La única columna con nulos es `poseLandmarks`: **860 registros (24,56 %)**. Se conservan para el análisis descriptivo y se excluyen solo para el modelo (8.4). No se imputan puntos corporales artificiales.

### 6.5. Eliminación de duplicados

| Control | Resultado |
|---|---|
| Filas completas duplicadas | 0 |
| Imágenes que no se pudieron abrir | 0 |
| Imágenes distintas por contenido (SHA256 de los píxeles) | 3.385 de 3.501 |

No hay filas exactamente repetidas, pero **116 imágenes repiten el contenido** de otra. Esta comprobación no puede hacerse comparando solo las filas: se decodifican las imágenes y se compara su huella. Los notebooks 01 a 03 **detectan y documentan** los duplicados sin eliminarlos, para conservar el dataset original; el notebook 04 los **trata**: aparta las etiquetas contradictorias y retira las copias exactas (9.2).

**Código** (notebook 01, sección 6.5):

```python
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
```

*Qué hace:* Decodifica cada imagen Base64, calcula la huella SHA256 de sus píxeles y compara el ancho y el alto reales con los declarados en el CSV. Así se detectan las imágenes repetidas aunque tengan otro nombre o etiqueta (116) y las dimensiones invertidas (3.218).

### 6.6. Tratamiento de datos inválidos

Se verificó que ningún registro tuviera etiqueta nula o vacía, ni ancho o alto nulos: **0 registros descartados**. Además, el notebook 04 aparta 141 registros cuya imagen aparece con ejercicios distintos (9.2). Los 860 registros sin pose se tratan según el uso: se mantienen para el análisis descriptivo y se excluyen para el modelo, que queda con 2.641 registros.

### 6.7. Conversión y corrección de tipos de datos

Las dimensiones se convierten a tipo `double` para calcular estadísticas. Al decodificar las imágenes se detectó que en **3.218 registros el ancho y el alto declarados están invertidos** respecto de la imagen real (otros 283 son cuadradas, donde no hay diferencia). Por ejemplo, una imagen declarada como 183 × 275 mide en realidad 275 × 183. Los notebooks 01 a 03 **no la corrigen** (las estadísticas de 7.1 se reportan tal como vienen en la fuente); el notebook 04 reemplaza el ancho y el alto por los valores reales de cada imagen.

### 6.8. Transformación de datos

La etiqueta se normaliza quitando espacios y pasando a minúsculas (`label = lower(trim(excercise))`), conservando la columna original `excercise`. No se encontraron etiquetas vacías.

### 6.9. Procesamiento mediante PySpark

El procesamiento se hace con una sesión de Spark en modo local. Primero se carga el CSV (conteo, esquema, nulos y duplicados con Spark, que coinciden con pandas) y, una vez validado el Parquet, se trabaja con él: normalización de la etiqueta, estadísticas, agregaciones y consultas.

**Qué hace Spark y qué hace pandas.** Spark realiza la carga del CSV, la validación, la normalización de la etiqueta, las estadísticas, las agregaciones, las consultas SQL, la extracción de las características y el modelamiento. pandas, PyArrow y Pillow se usan para decodificar las imágenes (una a una), para la depuración del dataset (notebook 04) y para escribir los archivos Parquet. La escritura nativa de Spark exige en Windows los binarios de Hadoop (winutils) y la depuración requiere procesar cada imagen, por lo que esas partes **no se distribuyeron**.

**Código** (notebook 01, sección 6.9):

```python
# multiLine y escape: el CSV tiene textos largos y comillas dentro de los campos
df_csv = (spark.read.option("header", True).option("inferSchema", True)
          .option("multiLine", True).option("escape", '"').csv(str(CSV_RAW)))
print("Registros:", df_csv.count(), "| columnas:", len(df_csv.columns))
df_csv.printSchema()
df_csv.drop("base64encoded").show(5, truncate=40)

# Nulos por columna y duplicados exactos, ahora con Spark (deben coincidir con pandas)
df_csv.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df_csv.columns]).show()
print("Duplicados exactos:", df_csv.count() - df_csv.dropDuplicates().count())
```

*Qué hace:* Carga el CSV con Spark (`multiLine` y `escape` porque hay textos largos y comillas dentro de los campos) y repite con Spark la comprobación de esquema, nulos y duplicados exactos, que debe coincidir con pandas.

### 6.10. Almacenamiento en formato Parquet

El CSV se convierte con pandas y PyArrow (`to_parquet`, sin índice). Se valida releyendo el archivo y comparando dimensiones y columnas con el original.

| Archivo | Tamaño |
|---|---|
| `imagePoses.csv` | 42,82 MiB |
| `imagePoses.parquet` | 39,25 MiB |

**Código** (notebook 01, sección 6.10):

```python
RUTA_PARQUET = PROCESSED / "imagePoses.parquet"
df.to_parquet(RUTA_PARQUET, engine="pyarrow", index=False)   # index=False: no guardar el índice como columna

# Validación: se relee el archivo y se compara con el DataFrame original
chequeo = pd.read_parquet(RUTA_PARQUET)
assert chequeo.shape == df.shape and chequeo.columns.tolist() == df.columns.tolist(), "El Parquet no coincide con el CSV"
print(f"Parquet: {RUTA_PARQUET}")
print(f"Tamaño: {mib(RUTA_PARQUET):.2f} MiB | filas: {len(chequeo)} | nulos en poseLandmarks: {int(chequeo['poseLandmarks'].isna().sum())}")
```

*Qué hace:* Escribe el Parquet con pandas y PyArrow (sin guardar el índice como columna) y lo valida releyéndolo: debe tener las mismas dimensiones y columnas que el CSV.

### 6.11. Particionamiento de los datos

El dataset se escribe en Parquet con compresión Snappy y **particionado por ejercicio** (`label`): 9 carpetas `label=<ejercicio>/`, con un archivo cada una, que suman 39,11 MiB. La lectura se valida con PyArrow y con Spark: ambos recuperan las 3.501 filas. Luego se empaqueta en `dataset_procesado.zip` (29,35 MiB).

Se escribe con PyArrow porque la escritura nativa de Spark exige en Windows los binarios de Hadoop (winutils). No se midió la mejora de rendimiento del particionamiento: con un dataset pequeño, muchas particiones pueden añadir sobrecarga.

**Código** (notebook 01, sección 6.11):

```python
# Se escribe con PyArrow: una carpeta label=<ejercicio>/ por categoría. No se usa la escritura de Spark
# porque en Windows requiere los binarios de Hadoop (winutils).
pq.write_to_dataset(pa.Table.from_pandas(df_proc, preserve_index=False), root_path=str(RUTA_PROCESADOS),
                    partition_cols=["label"], compression="snappy")

archivos = sorted(RUTA_PROCESADOS.rglob("*.parquet"))
print(f"Archivos Parquet: {len(archivos)} | tamaño total: {sum(a.stat().st_size for a in archivos) / MIB:.2f} MiB")
```

*Qué hace:* Escribe el dataset particionado por ejercicio: una carpeta `label=<ejercicio>/` por categoría, con compresión Snappy.

**Código** (notebook 01, sección 6.11):

```python
# Validación: PyArrow (particiones "hive") y Spark deben recuperar todas las filas escritas
n_arrow = ds.dataset(str(RUTA_PROCESADOS), format="parquet", partitioning="hive").to_table().num_rows
# Spark lee archivo por archivo (rutas explícitas) porque en Windows no puede listar carpetas sin winutils;
# basePath le permite reconstruir la columna "label" a partir de los nombres de carpeta. En Colab también funciona.
df_part = spark.read.option("basePath", str(RUTA_PROCESADOS)).parquet(*[str(a) for a in archivos])
n_spark = df_part.count()
print("Filas escritas:", len(df_proc), "| PyArrow:", n_arrow, "| Spark:", n_spark)
assert len(df_proc) == n_arrow == n_spark, "La lectura no recupera todas las filas"
```

*Qué hace:* Valida el resultado leyéndolo con PyArrow y con Spark. Spark lee archivo por archivo (`basePath` reconstruye la columna `label` a partir de las carpetas) y ambas lecturas deben recuperar todas las filas.

### 6.12. Comparación CSV vs. Parquet (tamaño y tiempo de consulta)

| | CSV | Parquet |
|---|---|---|
| Tamaño | 42,82 MiB | 39,25 MiB (**−8,33 %**) |
| Tiempo de consulta* | 0,46 s | 0,35 s |

\* Misma consulta (contar imágenes por ejercicio) en ambos formatos; mediana de 3 mediciones tras un calentamiento, **medida en el entorno local de verificación** (Spark en modo local). El notebook 01 imprime estos valores en cada ejecución y en Colab pueden variar. Con un archivo pequeño y una sola máquina la diferencia es pequeña (Parquet resultó algo más rápido): sirve como comparación, no como prueba de escalabilidad.

![Comparación de tamaños entre el CSV y el Parquet en la ejecución original](img/original_csv_vs_parquet.png)

*Figura 6.1. Salida de la comparación de tamaños en la ejecución original del pipeline: CSV 42,82 MB, Parquet 39,25 MB (−8,33 %).*

La reducción de tamaño es moderada porque la columna `base64encoded` (texto de imágenes ya comprimidas en JPEG) domina el tamaño y comprime poco. La ventaja del formato columnar aparece al leer solo algunas columnas.

**Código** (notebook 01, sección 6.12):

```python
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
```

*Qué hace:* Mide la misma consulta (contar imágenes por ejercicio) sobre el CSV y sobre el Parquet. Cada medición lee el archivo desde cero y se informa la mediana de tres, después de un calentamiento.

---

## 7. Análisis descriptivo
<!-- Responsable: Luis -->

### 7.1. Estadísticas descriptivas

Dimensiones **tal como vienen declaradas** en el CSV (con el ancho y el alto invertidos en 3.218 registros, ver 6.7):

| Columna | Promedio | Desv. estándar | Mínimo | Máximo |
|---|---|---|---|---|
| `imageWidth` (en realidad, el alto) | 191,14 | 34,50 | 102 | 400 |
| `imageHeight` (en realidad, el ancho) | 270,54 | 41,37 | 61 | 492 |

### 7.2. Distribución de los datos

| Ejercicio | Registros | % |
|---|---|---|
| bent over row, lunges, plank, pullup, shoulder press, squat | 400 (cada uno) | 11,43 |
| pushup | 398 | 11,37 |
| chest dips | 389 | 11,11 |
| glute bridge | 314 | 8,97 |
| **Total** | **3.501** | 100 |

Hay nueve categorías con representación casi pareja; *glute bridge* es la menos representada.

**Código** (notebook 01, sección 7.2):

```python
# 7.2 Distribución de ejercicios (cantidad y porcentaje)
total = df_base.count()
df_dist = (df_base.groupBy("label").agg(F.count("*").alias("cantidad"))
           .withColumn("porcentaje", F.round(F.col("cantidad") * 100 / total, 2))
           .orderBy(F.desc("cantidad"), "label"))   # "label" desempata para que el orden sea siempre el mismo
df_dist.show(truncate=False)
```

*Qué hace:* Agrupa por ejercicio con Spark y calcula la cantidad y el porcentaje; el desempate por `label` hace que el orden sea siempre el mismo.

### 7.3. Agrupaciones y agregaciones

Promedio de las dimensiones declaradas por ejercicio:

| Ejercicio | Registros | `imageWidth` (prom.) | `imageHeight` (prom.) |
|---|---|---|---|
| bent over row | 400 | 187,96 | 273,76 |
| lunges | 400 | 193,99 | 266,01 |
| plank | 400 | 185,74 | 275,24 |
| pullup | 400 | 196,85 | 265,36 |
| shoulder press | 400 | 192,70 | 269,22 |
| squat | 400 | 194,65 | 265,83 |
| pushup | 398 | 186,64 | 276,31 |
| chest dips | 389 | 189,85 | 273,17 |
| glute bridge | 314 | 192,01 | 269,94 |

Las diferencias entre ejercicios son pequeñas y descriptivas; no explican por sí solas el tipo de ejercicio.

### 7.4. Consultas mediante Spark SQL

Se registra el DataFrame como vista temporal (`ejercicios`) y se consulta con SQL. La primera consulta mide el porcentaje de imágenes sin `poseLandmarks` por ejercicio; la segunda calcula la participación de cada ejercicio con una función de ventana (`SUM(COUNT(*)) OVER ()`).

| Ejercicio | Imágenes | Sin pose | % sin pose |
|---|---|---|---|
| bent over row | 400 | 154 | 38,5 |
| chest dips | 389 | 117 | 30,1 |
| shoulder press | 400 | 114 | 28,5 |
| glute bridge | 314 | 84 | 26,8 |
| pushup | 398 | 89 | 22,4 |
| pullup | 400 | 85 | 21,3 |
| plank | 400 | 80 | 20,0 |
| squat | 400 | 70 | 17,5 |
| lunges | 400 | 67 | 16,8 |

El código de las consultas está en 14.1.

**Código** (notebook 01, sección 7.4):

```python
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
```

*Qué hace:* Registra el DataFrame como vista temporal y lo consulta con Spark SQL: el porcentaje de imágenes sin `poseLandmarks` por ejercicio y la participación de cada ejercicio con una función de ventana.

### 7.5. Identificación de patrones

- **Los datos faltantes no se reparten por igual:** *bent over row* pierde el 38,5 % de sus poses y *lunges* solo el 16,8 %. Al excluirlos para el modelo, las clases quedan desbalanceadas (de 333 registros de *lunges* a 230 de *glute bridge*).
- **Hay una clase menos representada** (*glute bridge*, 8,97 %).
- **Hay problemas de calidad en la fuente:** 116 imágenes repetidas y 3.218 registros con ancho y alto invertidos.

### 7.6. Visualizaciones

![Distribución de registros y participación porcentual por ejercicio](img/distribucion_ejercicios.png)

*Figura 7.1. Registros por ejercicio (izquierda) y participación porcentual (derecha).*

### 7.7. Interpretación del análisis descriptivo

El dataset está razonablemente equilibrado entre ejercicios, pero su calidad presenta tres alertas que condicionan el modelamiento: el 24,56 % sin pose (desigual entre ejercicios), las imágenes repetidas y las dimensiones invertidas. Las estadísticas de las dimensiones describen las imágenes, no la postura. Estos hallazgos justifican filtrar los registros sin pose antes de entrenar y evaluar con cuidado la posible repetición de imágenes entre entrenamiento y prueba (9.2).

---

## 8. Modelo predictivo
<!-- Responsable: Luis -->

### 8.1. Definición del problema predictivo

**Clasificación supervisada multiclase:** predecir cuál de los nueve ejercicios aparece en una imagen a partir de los puntos corporales estimados.

### 8.2. Variable objetivo

La etiqueta `label` (nueve categorías). Un `StringIndexer` la convierte en un índice numérico para el clasificador.

### 8.3. Selección de características

Se usan los **132 valores** de `poseLandmarks`: 33 puntos corporales × 4 números por punto (`x`, `y`, `z` y `visibility`). No se usan las dimensiones de la imagen ni la imagen misma.

**Cómo es el campo `poseLandmarks`.** Es un único texto (3.362 caracteres en el primer registro con pose, un *pushup*) con aspecto de lista de Python, que no es JSON válido. Cada elemento describe un punto del cuerpo:

```text
['x: 0.7909860014915466,y: 0.2797396183013916,z: -0.7337014675140381,visibility: 0.9999721050262451,',
 'x: 0.786137580871582,y: 0.24491778016090393,z: -0.699945330619812,visibility: 0.999929666519165,',
 ... (30 puntos más) ...
 'x: 0.10132566094398499,y: 0.7028536796569824,z: 0.6094520092010498,visibility: 0.9915837049484253,']
```

(Los saltos de línea se agregaron para que se lea; en el CSV el texto va seguido.) Tras la extracción de 8.4, cada registro queda como un vector de 132 valores, en el orden `x`, `y`, `z`, `visibility` de cada punto (aquí redondeados a dos decimales):

```text
[0.79, 0.28, -0.73, 1.00,   0.79, 0.24, -0.70, 1.00,   ...]   → 33 puntos × 4 = 132 valores
```

**Qué significan los valores.** `x` e `y` son coordenadas relativas al ancho y al alto de la imagen, por lo que lo esperable es que estén entre 0 y 1; sin embargo, en los 2.641 registros con pose hay valores fuera de ese rango (`x` entre −2,31 y 3,24; `y` entre −1,71 y 6,59), que son puntos que el modelo ubica fuera del encuadre. `z` es una profundidad relativa y `visibility` (entre 0,04 y 1,00 en los datos) indica qué tan visible es el punto.

**Cuáles son los 33 puntos.** El CSV no trae los nombres. La numeración corresponde al modelo de 33 puntos de MediaPipe Pose (Google, s. f.):

| Índices | Punto del cuerpo |
|---|---|
| 0 | Nariz |
| 1, 2, 3 | Ojo izquierdo (interno, centro y externo) |
| 4, 5, 6 | Ojo derecho (interno, centro y externo) |
| 7, 8 | Oreja izquierda y derecha |
| 9, 10 | Comisura de la boca izquierda y derecha |
| 11, 12 | Hombro izquierdo y derecho |
| 13, 14 | Codos |
| 15, 16 | Muñecas |
| 17, 18 | Meñiques de las manos |
| 19, 20 | Índices de las manos |
| 21, 22 | Pulgares |
| 23, 24 | Caderas |
| 25, 26 | Rodillas |
| 27, 28 | Tobillos |
| 29, 30 | Talones |
| 31, 32 | Punta del pie (una por pie) |

«Izquierdo» y «derecho» se refieren al cuerpo de la persona, no al lado de la imagen. Esta asignación se infiere de la coincidencia con ese modelo; el CSV no la declara. Se comprobó con los datos: en los 2.641 registros con pose, la altura media (`y`) de los puntos crece en el orden esperado (nariz 0,41; hombros 0,48; muñecas 0,59; caderas 0,73; rodillas 0,83; tobillos 0,98; puntas del pie 1,04), y al dibujar los puntos sobre una imagen real calzan con el cuerpo (Figura 8.1).

![Los 33 puntos de poseLandmarks dibujados sobre una imagen del dataset](img/ejemplo_pose_33_puntos.png)

*Figura 8.1. Los 33 puntos de `poseLandmarks` dibujados sobre una imagen del dataset (ejercicio *squat*); cada número es el índice del punto. Se usaron el ancho y el alto reales de la imagen: el CSV los declara como 168 × 299 y la imagen mide 299 × 168 (6.7). El código está en `informe/diagramas/generar_ejemplo_pose.py`.*

### 8.4. Preprocesamiento para Machine Learning

1. Se excluyen los 860 registros sin `poseLandmarks`: quedan **2.641** (de 3.501).
2. La etiqueta se presenta con mayúscula inicial (*Shoulder Press*).
3. `poseLandmarks` es un texto que **no es JSON**; se extraen los números con una expresión regular de Spark (`regexp_extract_all`), en el orden `x`, `y`, `z`, `visibility` de cada punto.
4. Se conservan las poses con la longitud más frecuente: **las 2.641 tienen 132 valores**.

Distribución de las clases con pose: *lunges* 333, *squat* 330, *plank* 320, *pullup* 315, *pushup* 309, *shoulder press* 286, *chest dips* 272, *bent over row* 246 y *glute bridge* 230.

**Código** (notebook 02, sección 8.4):

```python
NUMERO = "-?(?:[0-9]+(?:[.][0-9]*)?|[.][0-9]+)(?:[eE][+-]?[0-9]+)?"
df_car = (df_preparado
          .withColumn("features_array",
                      F.expr("transform(regexp_extract_all(poseLandmarks, '(?:x|y|z|visibility): *(" + NUMERO + ")', 1),"
                             " v -> cast(v as double))"))
          .filter(F.size("features_array") > 0))
```

*Qué hace:* Extrae los números de `poseLandmarks` con una expresión regular de Spark (`regexp_extract_all`), en el orden `x`, `y`, `z`, `visibility` de cada punto, y los convierte a `double`. Es una función nativa de Spark, no un UDF de Python.

### 8.5. Codificación de variables categóricas

Solo la variable objetivo es categórica y se codifica con `StringIndexer` (`handleInvalid="skip"`). No se requiere `OneHotEncoder`, porque todos los predictores son numéricos.

### 8.6. Vectorización de características

La lista de 132 números se convierte en un vector de MLlib con `array_to_vector`, que cumple la función de un `VectorAssembler` cuando las características ya vienen agrupadas en una lista.

**Código** (notebook 02, sección 8.6):

```python
df_modelo = (df_car.filter(F.size("features_array") == longitud)
                   .withColumn("features", array_to_vector("features_array"))   # lista de números -> vector de MLlib
                   .select("label", "features").cache())
print("Valores por pose:", longitud, "| registros para el modelo:", df_modelo.count())   # esperado: 132 | 2641
```

*Qué hace:* Conserva las poses con 132 valores y convierte la lista de números en un vector de MLlib con `array_to_vector`, que cumple la función de un `VectorAssembler`.

### 8.7. División de datos Train/Test

División aleatoria 80/20 con semilla 42: **2.145 registros de entrenamiento y 496 de prueba**. La división es por filas y **no es estratificada ni agrupa imágenes repetidas**: una misma foto podría quedar en ambos conjuntos, lo que puede afectar la evaluación (9.2 y 10.6). El resultado depende del número de núcleos de Spark; por eso la sesión se fija en 2 núcleos, con lo que la división es siempre la misma.

**Código** (notebook 02, sección 8.7):

```python
SEMILLA = 42
# División 80/20 al azar por filas. No es estratificada y no agrupa imágenes repetidas (hay 116 en el CSV),
# así que la misma foto puede quedar en entrenamiento y en prueba y la exactitud podría ser optimista.
entrenamiento, prueba = df_modelo.randomSplit([0.8, 0.2], seed=SEMILLA)
n_train, n_test = entrenamiento.count(), prueba.count()
print("Entrenamiento:", n_train, "| prueba:", n_test)   # referencia: 2145 | 496
```

*Qué hace:* División aleatoria 80/20 con semilla 42 (2.145 registros de entrenamiento y 496 de prueba).

### 8.8. Selección del algoritmo

**Random Forest** (Breiman, 2001) por: manejar bien variables numéricas sin escalado previo, ser robusto al sobreajuste, y estar disponible en `pyspark.ml`, lo que mantiene el entrenamiento en el mismo motor de las transformaciones. En el notebook 04 se compara con otros dos algoritmos (9.2); no se buscaron hiperparámetros.

### 8.9. Entrenamiento del modelo

Un `Pipeline` encadena el `StringIndexer` y el `RandomForestClassifier` con **50 árboles, profundidad máxima 10 y semilla 42**. El ajuste usa solo los 2.145 registros de entrenamiento y genera predicciones para los 496 de prueba.

**Código** (notebook 02, sección 8.9):

```python
modelo = Pipeline(stages=[
    StringIndexer(inputCol="label", outputCol="label_index", handleInvalid="skip"),       # texto -> número
    RandomForestClassifier(featuresCol="features", labelCol="label_index", numTrees=50, maxDepth=10, seed=SEMILLA),
]).fit(entrenamiento)                                # el ajuste usa solo los datos de entrenamiento
predicciones = modelo.transform(prueba).cache()
print("Predicciones:", predicciones.count())
```

*Qué hace:* Un `Pipeline` encadena el `StringIndexer` (etiqueta de texto a índice) y el Random Forest de 50 árboles y profundidad 10. El ajuste usa solo los datos de entrenamiento y luego se generan las predicciones sobre los de prueba.

---

## 9. Evaluación y resultados del modelo
<!-- Responsable: Richard -->

### 9.1. Generación de predicciones

El modelo ajustado se aplica a los **496 registros de prueba**. Cada predicción incluye el índice de la clase, la clase predicha y la probabilidad de cada clase.

### 9.2. Evaluación del modelo y validación cruzada

La evaluación se hizo en dos etapas.

**a) Partición única 80/20 (*hold-out*) sobre los datos originales** (notebook 02): exactitud 0,6875. Tiene dos limitaciones: depende de una sola división al azar y esta no impide que imágenes repetidas (116 en el CSV) queden en entrenamiento y en prueba.

**b) Validación cruzada de 5 grupos sobre un dataset depurado** (notebook 04). El propio notebook genera el dataset depurado a partir del CSV original: decodifica las 3.501 imágenes y calcula la huella SHA256 de cada una; **aparta los 141 registros con etiquetas contradictorias** (64 fotos que aparecían con dos ejercicios distintos); **retira 39 copias exactas** (se conserva la primera aparición); y **reemplaza el ancho y el alto declarados por los reales** (3.218 registros los tenían invertidos). Quedan **3.321 imágenes distintas**, que se asignan a **5 grupos (*folds*) estratificados por ejercicio, sin imágenes compartidas entre grupos** (semilla 42), y se guardan en Parquet. De ellas, 2.508 tienen pose (500, 511, 487, 500 y 510 por grupo). Lo apartado queda registrado en archivos para su revisión.

En cada vuelta cuatro grupos entrenan y uno prueba; tras las cinco vueltas cada imagen se usó una vez como prueba. Se evaluaron tres algoritmos de `pyspark.ml` con las mismas 132 características de pose: **Random Forest** (50 árboles, profundidad 10, los mismos parámetros del modelo original), **Árbol de decisión** (profundidad 10) y **Regresión logística** (100 iteraciones). Ninguno fue optimizado.

**Código** (notebook 04, sección 9.2):

```python
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
```

*Qué hace:* Depuración del dataset: aparta todas las filas de las imágenes con etiquetas contradictorias, retira las copias exactas (conserva la primera aparición) y comprueba que las cuentas cuadren y que cada imagen quede una sola vez.

**Código** (notebook 04, sección 9.2):

```python
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
```

*Qué hace:* Asigna cada imagen a uno de cinco grupos estratificados por ejercicio y comprueba que no haya imágenes compartidas entre entrenamiento y prueba y que los nueve ejercicios estén en ambos lados.

**Código** (notebook 04, sección 9.2):

```python
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
```

*Qué hace:* Validación cruzada: en cada vuelta un grupo es la prueba y los otros cuatro el entrenamiento, para los tres algoritmos, hasta que cada imagen se haya usado una vez como prueba. Se guardan las métricas de cada vuelta y las predicciones del Random Forest.

### 9.3. Métricas de evaluación

**Partición única 80/20 sobre los datos originales (notebook 02)**

| Métrica | Valor |
|---|---|
| **Exactitud (*accuracy*)** | **0,6875** (341 aciertos de 496) |
| Precisión ponderada | 0,7004 |
| *Recall* ponderado | 0,6875 |
| **F1 ponderado** | **0,6855** |

**Validación cruzada de 5 grupos sobre el dataset depurado (notebook 04): media ± desviación estándar de las 5 vueltas**

| Algoritmo | Exactitud | F1 ponderado | Precisión ponderada | *Recall* ponderado |
|---|---|---|---|---|
| **Regresión logística** | **0,8213 ± 0,0181** | 0,8210 ± 0,0185 | 0,8235 ± 0,0187 | 0,8213 ± 0,0181 |
| **Random Forest** | **0,7443 ± 0,0259** | 0,7434 ± 0,0258 | 0,7489 ± 0,0267 | 0,7443 ± 0,0259 |
| Árbol de decisión | 0,6037 ± 0,0171 | 0,6027 ± 0,0185 | 0,6103 ± 0,0197 | 0,6037 ± 0,0171 |

Exactitud en cada vuelta:

| Algoritmo | Grupo 1 | Grupo 2 | Grupo 3 | Grupo 4 | Grupo 5 |
|---|---|---|---|---|---|
| Regresión logística | 0,8380 | 0,8023 | 0,8029 | 0,8240 | 0,8392 |
| Random Forest | 0,7760 | 0,7378 | 0,7310 | 0,7120 | 0,7647 |
| Árbol de decisión | 0,6300 | 0,5871 | 0,6057 | 0,5900 | 0,6059 |

La regresión logística supera al Random Forest **en las cinco vueltas**. Como referencia adicional, una partición única 80/20 con el mismo método sobre el dataset depurado (2.058 de entrenamiento y 450 de prueba) dio una exactitud de 0,7222 y un F1 de 0,7213.

**Código** (notebook 02, sección 11.5):

```python
metricas = {}
for nombre, metrica in (("Accuracy", "accuracy"), ("Precisión ponderada", "weightedPrecision"),
                        ("Recall ponderado", "weightedRecall"), ("F1 ponderado", "f1")):
    evaluador = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName=metrica)
    metricas[nombre] = evaluador.evaluate(predicciones)
    print(f"{nombre}: {metricas[nombre]:.4f}")
```

*Qué hace:* Calcula las cuatro métricas del modelo (exactitud, precisión ponderada, *recall* ponderado y F1 ponderado) sobre las predicciones de prueba.

### 9.4. Visualización de resultados

**Matriz de confusión de la partición única original** (filas = ejercicio real; columnas = ejercicio predicho; los aciertos están en la diagonal):

| Real \ Predicho | Plank | Lunges | Pullup | Squat | Pushup | Shoulder Press | Chest Dips | Bent Over Row | Glute Bridge |
|---|---|---|---|---|---|---|---|---|---|
| **Bent Over Row** | 1 | 0 | 1 | 3 | 1 | 0 | 4 | **36** | 0 |
| **Chest Dips** | 0 | 0 | 1 | 1 | 2 | 0 | **33** | 5 | 0 |
| **Glute Bridge** | 7 | 9 | 0 | 2 | 3 | 0 | 2 | 3 | **29** |
| **Lunges** | 2 | **37** | 0 | 18 | 0 | 0 | 2 | 2 | 4 |
| **Plank** | **45** | 1 | 0 | 0 | 1 | 0 | 0 | 2 | 2 |
| **Pullup** | 0 | 0 | **37** | 1 | 0 | 7 | 4 | 0 | 0 |
| **Pushup** | 11 | 0 | 2 | 1 | **53** | 0 | 1 | 2 | 0 |
| **Shoulder Press** | 1 | 0 | 15 | 2 | 0 | **30** | 1 | 0 | 0 |
| **Squat** | 3 | 8 | 3 | **41** | 1 | 2 | 7 | 2 | 2 |

*Figura 9.1. Matriz de confusión sobre los 496 registros de prueba (notebook 02).*

![Validación cruzada de 5 grupos: exactitud por vuelta, media y desviación estándar](img/validacion_cruzada.png)

*Figura 9.2. Exactitud de cada algoritmo en las cinco vueltas de la validación cruzada (puntos), con su media y desviación estándar. La línea gris es la exactitud de la partición única original (0,6875).*

**Matriz de confusión global de la validación cruzada (Random Forest).** Cada imagen aparece una vez como prueba: 2.508 predicciones fuera de muestra.

| Real \ Predicho | Bent-over row | Chest dips | Glute bridge | Lunges | Plank | Pull-up | Push-up | Shoulder press | Squat |
|---|---|---|---|---|---|---|---|---|---|
| **Bent-over row** | **173** | 22 | 1 | 10 | 3 | 2 | 12 | 0 | 13 |
| **Chest dips** | 19 | **213** | 3 | 2 | 2 | 9 | 4 | 4 | 9 |
| **Glute bridge** | 7 | 3 | **162** | 21 | 22 | 0 | 5 | 0 | 4 |
| **Lunges** | 3 | 13 | 15 | **220** | 10 | 1 | 1 | 1 | 45 |
| **Plank** | 5 | 3 | 18 | 6 | **223** | 1 | 23 | 3 | 2 |
| **Pull-up** | 2 | 13 | 0 | 0 | 0 | **242** | 2 | 46 | 2 |
| **Push-up** | 4 | 10 | 6 | 2 | 29 | 4 | **234** | 1 | 4 |
| **Shoulder press** | 1 | 9 | 0 | 0 | 2 | 64 | 4 | **182** | 17 |
| **Squat** | 9 | 19 | 4 | 44 | 4 | 5 | 2 | 5 | **218** |

*Figura 9.3. Matriz de confusión global del Random Forest en la validación cruzada (exactitud 0,7444).*

**Recall y precisión por ejercicio (Random Forest, validación cruzada):**

| Ejercicio | Registros | *Recall* | Precisión |
|---|---|---|---|
| Shoulder press | 279 | 0,652 | 0,752 |
| Squat | 310 | 0,703 | 0,694 |
| Lunges | 309 | 0,712 | 0,721 |
| Glute bridge | 224 | 0,723 | 0,775 |
| Bent-over row | 236 | 0,733 | 0,776 |
| Plank | 284 | 0,785 | 0,756 |
| Pull-up | 307 | 0,788 | 0,738 |
| Push-up | 294 | 0,796 | 0,815 |
| Chest dips | 265 | 0,804 | 0,698 |

### 9.5. Análisis e interpretación de las métricas de cara al negocio

El dominio es una aplicación educativa de análisis de ejercicios por visión artificial, donde el modelo debería reconocer qué ejercicio hace la persona para luego evaluar su técnica.

- **Qué significan las cifras:** una exactitud de 0,82 (regresión logística) equivale a unas 8 imágenes bien clasificadas de cada 10; con Random Forest (0,74) son unas 7 de cada 10. Sirve como primer filtro o sugerencia, pero **no basta para una corrección automática** sin confirmación.
- **Las confusiones se concentran en ejercicios de posturas parecidas** (matriz global): *Shoulder press* → *Pull-up* (64 de 279) y *Pull-up* → *Shoulder press* (46 de 307); *Lunges* → *Squat* (45 de 309) y *Squat* → *Lunges* (44 de 310); *Push-up* → *Plank* (29 de 294) y *Plank* → *Push-up* (23 de 284); *Glute bridge* → *Plank* (22) y *Lunges* (21). Es una hipótesis razonable que se parezcan en la disposición de los brazos sobre la cabeza, de las piernas o del apoyo horizontal; **no se verificó**.
- **El rendimiento por ejercicio es más parejo que en la partición única:** el *recall* va de 0,65 (*Shoulder press*) a 0,80 (*Chest dips*).
- **La estimación tiene incertidumbre:** la exactitud del Random Forest varía entre 0,712 y 0,776 según el grupo de prueba (±2,6 puntos). Diferencias de uno o dos puntos entre modelos no son concluyentes; la ventaja de la regresión logística sobre el Random Forest (unos 8 puntos, en las cinco vueltas) sí lo es.
- **Implicancia práctica:** si el reconocimiento del ejercicio falla, se aplicarían las reglas de otro movimiento. Convendría mostrar la probabilidad de la predicción y pedir confirmación al usuario cuando sea baja.

### 9.6. Desempeño del modelo

Con la validación cruzada, el desempeño es **moderado a bueno**: 0,74 con Random Forest y 0,82 con regresión logística; el árbol de decisión individual queda en 0,60. Es compatible con que las coordenadas normalizadas de la pose se separen bien con fronteras lineales, mientras que el Random Forest, con 50 árboles de profundidad 10 y unos 2.000 registros de entrenamiento, rinde menos; **esta explicación es una hipótesis que no se verificó**. Ninguno de los modelos se optimizó (no hay búsqueda de hiperparámetros).

**Comparación con el resultado original.** La evaluación con datos depurados dio un resultado **mayor** que la partición única original (0,7222 en partición única y 0,7443 ± 0,0259 en validación cruzada, frente a 0,6875). Por lo tanto, la sospecha de que las imágenes repetidas inflaban la exactitud original **no se confirma**: los resultados son compatibles con que las etiquetas contradictorias (141 registros, 64 fotos con dos ejercicios distintos) y las copias exactas perjudicaran el aprendizaje y la evaluación. La comparación no aísla el efecto de cada factor, porque cambian a la vez los datos y el conjunto de prueba.

**Límites de la evaluación:** no hay identificador de persona, así que los grupos evitan imágenes idénticas pero una misma persona podría aparecer en entrenamiento y en prueba; por eso los resultados no prueban la generalización a personas nuevas. Las etiquetas no se revisaron una por una. El 24 % de las imágenes originales no pudo usarse por no tener pose.

### 9.7. Comparación entre resultados esperados y obtenidos

Cada notebook contrasta sus resultados con los valores de referencia de la ejecución original. Ejecutados en Colab, en Supabase y en Databricks, **todas las comprobaciones coincidieron**.

| Notebook | Comprobaciones | Resultado |
|---|---|---|
| 01 (Colab) | 10: filas, columnas, nulos, duplicados, categorías, tamaños de CSV, Parquet, particiones y ZIP | ✔ 10 de 10 |
| 02 (Colab + Supabase) | 9: registros, Parquet, total en Supabase, poses, 132 valores, entrenamiento/prueba, exactitud y F1 | ✔ 9 de 9 |
| 03 (Databricks) | 8: Parquet, registros, columnas, categorías y 3 cantidades por ejercicio | ✔ 8 de 8 |
| 04 (depuración y validación cruzada) | 13: imágenes distintas, dimensiones, contradicciones, copias, registros depurados, grupos, imágenes compartidas y vueltas | ✔ 13 de 13 (ejecutado en Colab y en local, con los mismos resultados) |

**Comparación entre evaluaciones (Random Forest)**

| Evaluación | Datos | Exactitud |
|---|---|---|
| Partición única 80/20 (notebook 02) | 3.501 originales (2.641 con pose) | 0,6875 |
| Partición única 80/20, mismo método | 3.321 depurados (2.508 con pose) | 0,7222 |
| Validación cruzada de 5 grupos (notebook 04) | 3.321 depurados (2.508 con pose) | 0,7443 ± 0,0259 |

---

## 10. Resultados generales y discusión
<!-- Responsable: Richard -->

### 10.1. Resultados del procesamiento de datos

- Se procesaron los **3.501 registros** (análisis y transformaciones con PySpark) y se almacenaron en **Parquet con Snappy, particionado por ejercicio** (9 particiones, 39,11 MiB), frente a los 42,82 MiB del CSV.
- Se detectaron problemas de calidad: 860 registros sin pose, 116 imágenes repetidas por contenido y 3.218 registros con las dimensiones invertidas.
- El resultado se validó en tres entornos (Colab, Supabase y Databricks) y se resumió en una tabla de Supabase de 9 filas que suman 3.501.

### 10.2. Resultados del análisis descriptivo

El dataset tiene nueve ejercicios con representación casi pareja (314 a 400 registros). Los datos faltantes de pose son desiguales entre ejercicios (16,8 % a 38,5 %), lo que desbalancea las clases al preparar el modelo.

### 10.3. Resultados del modelo predictivo

Random Forest (50 árboles, profundidad 10) sobre 132 características de pose: **exactitud 0,6875 y F1 ponderado 0,6855** en la partición única original (496 registros de prueba). Con el dataset depurado y validación cruzada de 5 grupos: **Random Forest 0,7443 ± 0,0259**, **regresión logística 0,8213 ± 0,0181** y árbol de decisión 0,6037 ± 0,0171. La regresión logística fue mejor en las cinco vueltas.

### 10.4. Cumplimiento de los objetivos

| Objetivo específico | Resultado | Estado |
|---|---|---|
| 1. Arquitectura y alta disponibilidad | Diagramas (3.6 y 3.8) y explicación de mecanismos. La alta disponibilidad se presenta como diseño, no se demostró | Cumplido (diseño) |
| 2. Ingesta y procesamiento con PySpark | Notebook 01: análisis, SQL y transformaciones con Spark; decodificación de imágenes y escritura de Parquet con pandas y PyArrow | Cumplido |
| 3. Parquet, particionamiento y comparación con CSV | Tamaño (−8,33 %) y tiempo de consulta medidos | Cumplido |
| 4. Análisis descriptivo con Spark SQL | Secciones 7.1 a 7.7 | Cumplido |
| 5. Modelo Random Forest evaluado | Exactitud 0,6875 (partición única) y 0,7443 ± 0,0259 (validación cruzada, notebook 04); comparación con otros dos algoritmos | Cumplido |
| 6. Integración con Supabase y Databricks | Notebooks 02 y 03 ejecutados en ambos | Cumplido |
| 7. Planificación y repositorio | Gantt, roles y bitácora; repositorio: enlace en 11.6 | Cumplido |

### 10.5. Análisis de la solución implementada

**Fortalezas**
- Reproducible: los notebooks son idempotentes, verifican sus resultados y se ejecutan en Colab y Databricks.
- Formato eficiente y credenciales fuera del código.
- Cubre todas las capas con servicios gratuitos.

**Debilidades**
- No es un clúster: Spark corre en modo local con 2 núcleos.
- La transferencia del ZIP entre plataformas es manual.
- No hay orquestación ni monitoreo.

### 10.6. Limitaciones del proyecto

1. **Volumen pequeño** (43 MiB, 3.501 registros): no demuestra el comportamiento con grandes volúmenes ni la escalabilidad.
2. **Alta disponibilidad no demostrada:** los servicios gratuitos no la garantizan y Supabase Free no tiene respaldos.
3. **Calidad del dataset:** 116 imágenes repetidas, 141 registros con etiquetas contradictorias, dimensiones invertidas y 24,56 % sin pose. Los notebooks 01 a 03 trabajan con el dataset original (solo detectan los problemas); el 04 depura el dataset y trabaja con el resultado, por lo que las evaluaciones no usan exactamente los mismos datos. Las etiquetas se heredan de la fuente, sin revisión visual (la confusión entre ejercicios parecidos podría deberse en parte a errores de etiquetado).
4. **Evaluación del modelo:** la partición única original no controla duplicados; la validación cruzada usa grupos sin imágenes idénticas, pero no hay identificador de persona, por lo que no prueba la generalización a personas nuevas.
5. **Modelos sin optimizar:** se compararon tres algoritmos, pero sin búsqueda de hiperparámetros.
6. **Procesamiento solo *batch*:** no se implementó *streaming*.
7. **Dependencia de plataformas de nivel gratuito:** sesiones temporales y capacidad limitada.
8. **Depuración no distribuida:** la decodificación de las imágenes, el cálculo de huellas y la depuración se hacen con pandas y Pillow en una sola máquina; Spark se usa para el análisis, las características y el modelo.

### 10.7. Posibles mejoras y trabajo futuro

- **Incorporar la depuración al pipeline principal:** hoy la depuración está en el notebook 04; convendría llevarla al notebook 01 para que el resumen de Supabase, el ZIP de Databricks y el modelo del notebook 02 usen también el dataset depurado.
- **Búsqueda de hiperparámetros** con `CrossValidator` de `pyspark.ml` y más algoritmos; en la validación cruzada la regresión logística superó al Random Forest sin ser optimizada.
- **Prueba de escalabilidad** con un volumen mayor, declarado como sintético y usado solo para medir rendimiento, nunca para las métricas del modelo.
- **Almacenamiento en la nube de objetos** (Google Cloud Storage o Amazon S3) y consultas con BigQuery o Athena.
- **Orquestación y monitoreo** (por ejemplo, Airflow o Databricks Jobs) y *streaming* con *Structured Streaming*.
- **Evaluación por persona** para medir la generalización.
- **Distribuir la depuración:** llevar a Spark la parte relacional (agrupar por huella para detectar contradicciones y copias) y calcular las huellas con una función por lotes, de modo que escale con volúmenes mayores.

---

## 11. Evidencias de implementación
<!-- Responsable: Luis -->

Las capturas corresponden a las ejecuciones de los notebooks en Google Colab, Supabase y Databricks (17 de octubre de 2026).

### 11.1. Evidencias del funcionamiento en Google Colab

**Notebook 01: ingesta, análisis y almacenamiento**

![Notebook 01: ZIP generado](img/colab_01_zip.png)

*Figura 11.1. Notebook 01: generación de `dataset_procesado.zip` (29,35 MiB, 9 archivos) y del resumen por ejercicio. En Colab, el ZIP se descarga al equipo local.*

![Notebook 01: tabla de verificación](img/colab_01_verificacion.png)

*Figura 11.2. Notebook 01: tabla de verificación de resultados, con las 10 comprobaciones en ✔.*

**Notebook 02: integración con Supabase y modelo**

![Notebook 02: entorno](img/colab_02_entorno.png)

*Figura 11.3. Notebook 02: instalación de dependencias (`pyspark==3.5.3`, `psycopg`, `setuptools`) y sesión de Spark 3.5.3 en modo local con 2 núcleos.*

![Notebook 02: carga del ZIP](img/colab_02_carga_zip.png)

*Figura 11.4. Notebook 02: carga de `dataset_procesado.zip` (30.770.505 bytes) y lectura con Spark: 9 archivos Parquet, 3.501 registros y 7 columnas.*

### 11.2. Evidencias de servicios Cloud

**Supabase (PostgreSQL)**

![Proyecto de Supabase](img/supabase_proyecto.png)

*Figura 11.5. Proyecto en Supabase: plan Free, región East US (North Virginia) y estado Healthy.*

![Conexión mediante Session pooler](img/supabase_connect_session_pooler.png)

*Figura 11.6. Método de conexión elegido: Session pooler, tipo URI.*

![Parámetros de conexión](img/supabase_connect_parametros.png)

*Figura 11.7. Parámetros de conexión del pooler (host, puerto, base y usuario; sin la contraseña).*

![Secretos de Colab](img/colab_secretos.png)

*Figura 11.8. Secreto `SUPABASE_DB_URL` en Colab, con el valor oculto y el acceso habilitado para el cuaderno.*

![Conexión desde Colab](img/colab_02_conexion.png)

*Figura 11.9. Conexión desde Colab al Session pooler: se muestran host, puerto y usuario (nunca la contraseña) y una consulta de prueba exitosa.*

![Tabla en Supabase](img/supabase_tabla.png)

*Figura 11.10. Tabla `resumen_ejercicios` en el Table Editor de Supabase: 9 filas, una por ejercicio.*

**Databricks Free Edition**

![Catálogo de Databricks](img/databricks_catalogo.png)

*Figura 11.11. Catálogo por defecto (`workspace`) de la Free Edition.*

![Creación del volumen](img/databricks_crear_volumen.png)

*Figura 11.12. Creación del volumen `dataset` (administrado) en `workspace.default`.*

![Volumen de Databricks](img/databricks_volumen.png)

*Figura 11.13. Volumen `/Volumes/workspace/default/dataset`.*

![ZIP cargado en el volumen](img/databricks_zip_subido.png)

*Figura 11.14. `dataset_procesado.zip` cargado en el volumen.*

![Notebook importado](img/databricks_notebook_importado.png)

*Figura 11.15. Notebook 03 importado en Databricks, con cómputo *serverless*.*

![Notebook 03: tabla de verificación](img/databricks_03_verificacion.png)

*Figura 11.16. Notebook 03: tabla de verificación con las 8 comprobaciones en ✔ (9 Parquet, 3.501 registros, 7 columnas y 9 categorías).*

![Notebook 03: lectura del dataset](img/databricks_03_lectura.png)

*Figura 11.17. Notebook 03: lectura del dataset con Spark en Databricks: esquema y distribución por ejercicio con su porcentaje.*

### 11.3. Evidencias del procesamiento

Los resultados del procesamiento (conteos, nulos, duplicados, esquema, estadísticas y consultas SQL) están en las secciones 6 y 7, y se reproducen ejecutando el notebook 01. Se muestran las evidencias de la preparación de datos del notebook 02 y del resumen guardado en Supabase, que se comprobó releyendo la tabla con una conexión nueva después del *commit*.

![Preparación de las poses](img/colab_02_preparacion.png)

*Figura 11.18. Notebook 02: preparación de las poses: 3.501 registros, 2.641 con pose y 860 excluidos, con la distribución por ejercicio.*

![Extracción de características](img/colab_02_caracteristicas.png)

*Figura 11.19. Notebook 02: extracción de características: las 2.641 poses tienen 132 valores.*

![Resumen guardado en Supabase](img/colab_02_resumen_supabase.png)

*Figura 11.20. Notebook 02: resumen por ejercicio guardado y releído desde Supabase: 9 categorías y un total de 3.501 registros.*

### 11.4. Evidencias del almacenamiento Parquet

Parquet inicial de 39,25 MiB, 9 particiones por ejercicio con Snappy (39,11 MiB) y ZIP de 29,35 MiB, comprobados por la tabla de verificación del notebook 01 (Figuras 11.1 y 11.2) y leídos con Spark en Databricks (Figuras 11.16 y 11.17).

![Comparación de tamaños CSV y Parquet](img/original_csv_vs_parquet.png)

*Figura 11.21. Comparación de tamaños entre el CSV y el Parquet en la ejecución original (42,82 MB frente a 39,25 MB; −8,33 %).*

### 11.5. Evidencias del modelo predictivo

Las métricas, la matriz de confusión y la tabla por ejercicio están en 9.3 y 9.4. Se muestran las salidas del notebook 02 y las del notebook 04 (depuración y validación cruzada).

![Métricas y matriz de confusión](img/colab_02_metricas_matriz.png)

*Figura 11.22. Notebook 02: métricas del modelo (exactitud 0,6875, F1 ponderado 0,6855) y matriz de confusión.*

![Recall y precisión por ejercicio](img/colab_02_por_ejercicio.png)

*Figura 11.23. Notebook 02: *recall* y precisión por ejercicio.*

![Muestras: esquema y poseLandmarks](img/colab_02_muestras_esquema.png)

*Figura 11.24. Notebook 02: esquema del DataFrame y ejemplos del texto de `poseLandmarks`.*

![Muestras: características extraídas](img/colab_02_muestras_pose.png)

*Figura 11.25. Notebook 02: texto de `poseLandmarks` ya preparado y listas de números extraídas.*

![Muestras: predicciones](img/colab_02_muestras_predicciones.png)

*Figura 11.26. Notebook 02: predicciones con la probabilidad de cada clase y ejemplos de ejercicio real frente al predicho.*

![Notebook 04: descarga del dataset](img/colab_04_kagglehub.png)

*Figura 11.27. Notebook 04: el dataset se descarga con KaggleHub (42,82 MiB, 3.501 registros, 6 columnas), sin credenciales.*

![Notebook 04: resultados por vuelta](img/colab_04_resultados_por_vuelta.png)

*Figura 11.28. Notebook 04: resultados de las cinco vueltas de la validación cruzada para cada algoritmo.*

![Notebook 04: media y desviación estándar](img/colab_04_resumen.png)

*Figura 11.29. Notebook 04: media y desviación estándar por algoritmo (Random Forest: exactitud 0,7443 ± 0,0259).*

![Notebook 04: comparación entre evaluaciones](img/colab_04_comparacion.png)

*Figura 11.30. Notebook 04: comparación entre la partición única original (0,6875), la partición única sobre datos depurados (0,7222) y la validación cruzada (0,7443 ± 0,0259).*

![Notebook 04: tabla de verificación](img/colab_04_verificacion.png)

*Figura 11.31. Notebook 04: tabla de verificación con las 13 comprobaciones de la depuración y de la validación cruzada en ✔.*

### 11.6. Estructura del repositorio GitHub

Enlace: https://github.com/Richard-Rocuant-O/apache-spark-ml

```
apache-spark-ml/
├── README.md                  (descripción, orden de ejecución y requisitos)
├── requirements.txt           (PySpark 3.5.3, pandas, pyarrow, pillow, kagglehub, matplotlib, scikit-learn, psycopg[binary], setuptools)
├── .gitignore                 (excluye datos, ZIP y credenciales)
├── notebooks/
│   ├── 01_Ingesta_Limpieza_Analisis_Almacenamiento_Datos.ipynb
│   ├── 02_Supabase_Random_Forest.ipynb
│   ├── 03_Validacion_Databricks.ipynb
│   └── 04_Depuracion_Validacion_Cruzada.ipynb
├── docs/                      (informe, capturas, diagramas y carta Gantt)
└── scripts/                   (versiones .py de los notebooks)
```

El repositorio **no** incluye el CSV, el ZIP ni credenciales.

---

## 12. Conclusiones
<!-- Responsable: Richard -->

### 12.1. Conclusión general

Se diseñó e implementó un pipeline de Big Data *batch* bajo la Opción C que ingiere, valida, transforma, almacena y analiza un dataset de 3.501 imágenes de ejercicios con sus puntos corporales, y entrena clasificadores con `pyspark.ml`. El Random Forest alcanzó una exactitud de 0,6875 en la partición única original y de 0,7443 ± 0,0259 en validación cruzada sobre el dataset depurado; la regresión logística obtuvo 0,8213 ± 0,0181. Los notebooks se ejecutaron en Colab, Supabase y Databricks, y sus resultados coinciden con los valores de referencia.

### 12.2. Conclusiones técnicas

- El formato **Parquet particionado con Snappy** redujo el tamaño en un 8,33 % respecto del CSV; la mejora es moderada porque el dataset está dominado por texto Base64.
- **Spark SQL y DataFrames** permitieron un análisis descriptivo declarativo y revelaron un patrón útil: los datos de pose faltan de forma desigual entre ejercicios.
- El modelo logra un desempeño **moderado a bueno** (0,69 en la partición original; 0,74 con Random Forest y 0,82 con regresión logística en validación cruzada), con confusiones entre ejercicios de posturas parecidas. La validación cruzada permitió comparar algoritmos con una estimación de su variabilidad (±2 puntos) y mostró que un modelo lineal sencillo supera al Random Forest sin optimizar.
- La depuración del dataset (141 registros con etiquetas contradictorias y 39 copias exactas) coincide con una evaluación más alta, lo que sugiere que esos defectos perjudicaban más de lo que inflaban el resultado.
- La **reproducibilidad** exigió decisiones técnicas concretas: versiones fijadas, rutas unificadas, núcleos de Spark fijos para una división estable y notebooks idempotentes.
- Verificar el contenido de las imágenes, y no solo las filas, reveló duplicados y dimensiones invertidas que una revisión superficial no detecta.

### 12.3. Aportes del proyecto

- Un pipeline reproducible y verificable de extremo a extremo con servicios gratuitos, con comprobaciones automáticas de sus resultados.
- Un diagnóstico de la calidad del dataset.
- Una bitácora de incidencias técnicas reales y sus soluciones, útil para quien repita el trabajo.

### 12.4. Proyección futura

El trabajo es la base de una aplicación educativa de análisis de ejercicios por visión artificial. Los próximos pasos son depurar el dataset, validar el modelo con validación cruzada y con evaluación por persona, medir la escalabilidad con mayores volúmenes, migrar el almacenamiento a un *Data Lake* en la nube y agregar procesamiento en tiempo real.

---

## 13. Referencias bibliográficas
<!-- Responsable: Richard · Formato APA 7 · [PENDIENTE: verificar los datos de cada referencia antes de entregar] -->

Apache Parquet. (s. f.). *Apache Parquet documentation*. https://parquet.apache.org/docs/

Armbrust, M., Xin, R. S., Lian, C., Huai, Y., Liu, D., Bradley, J. K., Meng, X., Kaftan, T., Franklin, M. J., Ghodsi, A., & Zaharia, M. (2015). Spark SQL: Relational data processing in Spark. En *Proceedings of the 2015 ACM SIGMOD International Conference on Management of Data* (pp. 1383-1394). ACM.

asher213. (s. f.). *Exercise images with pose estimation* [Conjunto de datos]. Kaggle. https://www.kaggle.com/datasets/asher213/exercise-images-with-pose-estimation

Breiman, L. (2001). Random forests. *Machine Learning, 45*(1), 5-32.

Dean, J., & Ghemawat, S. (2004). MapReduce: Simplified data processing on large clusters. En *Proceedings of the 6th Symposium on Operating Systems Design and Implementation (OSDI '04)* (pp. 137-150). USENIX.

Google. (s. f.). *Pose landmark detection guide*. Google AI Edge. https://developers.google.com/edge/mediapipe/solutions/vision/pose_landmarker

Laney, D. (2001). *3D data management: Controlling data volume, velocity, and variety* (Research note). META Group.

Mell, P., & Grance, T. (2011). *The NIST definition of cloud computing* (NIST Special Publication 800-145). National Institute of Standards and Technology.

Meng, X., Bradley, J., Yavuz, B., Sparks, E., Venkataraman, S., Liu, D., Freeman, J., Tsai, D. B., Amde, M., Owen, S., Xin, D., Xin, R., Franklin, M. J., Zadeh, R., Zaharia, M., & Talwalkar, A. (2016). MLlib: Machine learning in Apache Spark. *Journal of Machine Learning Research, 17*(34), 1-7.

Zaharia, M., Chowdhury, M., Das, T., Dave, A., Ma, J., McCauley, M., Franklin, M. J., Shenker, S., & Stoica, I. (2012). Resilient distributed datasets: A fault-tolerant abstraction for in-memory cluster computing. En *Proceedings of the 9th USENIX Symposium on Networked Systems Design and Implementation (NSDI '12)* (pp. 15-28). USENIX.

Zaharia, M., Xin, R. S., Wendell, P., Das, T., Armbrust, M., Dave, A., Meng, X., Rosen, J., Venkataraman, S., Franklin, M. J., Ghodsi, A., Gonzalez, J., Shenker, S., & Stoica, I. (2016). Apache Spark: A unified engine for big data processing. *Communications of the ACM, 59*(11), 56-65.

---

## 14. Anexos

### 14.1. Código fuente relevante

El código completo y documentado está en los notebooks del repositorio (11.6). Los fragmentos principales se muestran, con su explicación, junto a cada sección (5.1, 5.4, 5.5, 6.1, 6.5, 6.9 a 6.12, 7.2, 7.4, 8.4 a 8.9, 9.2 y 9.3); aquí se reúnen los más relevantes.

**Escritura de Parquet particionado con Snappy (notebook 01)**

```python
pq.write_to_dataset(pa.Table.from_pandas(df_proc, preserve_index=False),
                    root_path=str(RUTA_PROCESADOS),
                    partition_cols=["label"], compression="snappy")
```

**Consultas con Spark SQL (notebook 01)**

```python
df_base.createOrReplaceTempView("ejercicios")

spark.sql("""
    SELECT label,
           COUNT(*)                                                                  AS imagenes,
           SUM(CASE WHEN poseLandmarks IS NULL THEN 1 ELSE 0 END)                    AS sin_pose,
           ROUND(100.0 * AVG(CASE WHEN poseLandmarks IS NULL THEN 1 ELSE 0 END), 1)  AS pct_sin_pose
    FROM ejercicios
    GROUP BY label
    ORDER BY pct_sin_pose DESC
""").show(truncate=False)

spark.sql("""
    SELECT label, COUNT(*) AS cantidad,
           ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS porcentaje
    FROM ejercicios
    GROUP BY label
    ORDER BY cantidad DESC, label
""").show(truncate=False)
```

**Tabla en Supabase (notebook 02)**

```python
cur.execute("""CREATE TABLE IF NOT EXISTS resumen_ejercicios (
                   ejercicio TEXT PRIMARY KEY,
                   cantidad  INTEGER NOT NULL)""")
cur.execute("ALTER TABLE resumen_ejercicios ENABLE ROW LEVEL SECURITY")
cur.executemany("""INSERT INTO resumen_ejercicios (ejercicio, cantidad) VALUES (%s, %s)
                   ON CONFLICT (ejercicio) DO UPDATE SET cantidad = EXCLUDED.cantidad""", resumen)
```

**Extracción de las 132 características y entrenamiento (notebook 02)**

```python
NUMERO = "-?(?:[0-9]+(?:[.][0-9]*)?|[.][0-9]+)(?:[eE][+-]?[0-9]+)?"
df_car = (df_preparado
          .withColumn("features_array",
                      F.expr("transform(regexp_extract_all(poseLandmarks, '(?:x|y|z|visibility): *(" + NUMERO + ")', 1),"
                             " v -> cast(v as double))"))
          .filter(F.size("features_array") > 0))

entrenamiento, prueba = df_modelo.randomSplit([0.8, 0.2], seed=42)
modelo = Pipeline(stages=[
    StringIndexer(inputCol="label", outputCol="label_index", handleInvalid="skip"),
    RandomForestClassifier(featuresCol="features", labelCol="label_index", numTrees=50, maxDepth=10, seed=42),
]).fit(entrenamiento)
```

### 14.2. Capturas adicionales

Todas las capturas se incluyen en el capítulo 11; los archivos de imagen están en la carpeta `img/` del repositorio.

### 14.3. Configuraciones utilizadas

| Parámetro | Valor |
|---|---|
| Spark / PySpark | 3.5.3 |
| Modo de Spark | Local: `local[*]` (notebook 01) y `local[2]` (notebook 02) |
| `spark.sql.shuffle.partitions` | 4 |
| `spark.driver.memory` | 2 GB |
| Parquet | PyArrow, compresión Snappy, particionado por `label` |
| División train/test | 80 % / 20 %, semilla 42 |
| Depuración y validación cruzada (notebook 04) | Huella SHA256 de la imagen; 5 grupos estratificados (`StratifiedKFold`, semilla 42); `local[2]`; Random Forest (50 árboles, profundidad 10), árbol de decisión (profundidad 10) y regresión logística (100 iteraciones) |
| Random Forest | 50 árboles, profundidad máxima 10, semilla 42 |
| `StringIndexer` | `handleInvalid="skip"` |
| Conexión a Supabase | *Session pooler*, puerto 5432, `sslmode=require` |
| Secreto | `SUPABASE_DB_URL` (Secretos de Colab) |
| Volumen de Databricks | `/Volumes/workspace/default/dataset` |
| Verificación local | Python 3.12, JDK 17, pandas 3.0.6, NumPy 2.5.4, PyArrow 26.0.0 |

### 14.4. Enlace al repositorio GitHub

https://github.com/Richard-Rocuant-O/apache-spark-ml (debe ir también como enlace dentro del PDF).
