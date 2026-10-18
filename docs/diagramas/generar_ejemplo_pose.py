"""Dibuja los 33 puntos de `poseLandmarks` sobre una imagen real del dataset (Figura 8.1 del informe).

Uso:  python generar_ejemplo_pose.py [ruta/a/imagePoses.csv]
Salida: ../img/ejemplo_pose_33_puntos.png
Imprime además la altura media (`y`) de algunos puntos y los rangos de los valores, que se citan en 8.3.
"""
import base64
import io
import re
import sys
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd
from PIL import Image

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

AQUI = Path(__file__).resolve().parent
CSV = Path(sys.argv[1]) if len(sys.argv) > 1 else AQUI.parents[1] / "content" / "proyecto_big_data" / "raw" / "imagePoses.csv"
SALIDA = AQUI.parent / "img" / "ejemplo_pose_33_puntos.png"

NUMERO = re.compile(r"-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?")
NOMBRES = ["nariz", "ojo izq. interno", "ojo izq.", "ojo izq. externo", "ojo der. interno", "ojo der.", "ojo der. externo",
           "oreja izq.", "oreja der.", "boca izq.", "boca der.", "hombro izq.", "hombro der.", "codo izq.", "codo der.",
           "muñeca izq.", "muñeca der.", "meñique izq.", "meñique der.", "índice izq.", "índice der.", "pulgar izq.",
           "pulgar der.", "cadera izq.", "cadera der.", "rodilla izq.", "rodilla der.", "tobillo izq.", "tobillo der.",
           "talón izq.", "talón der.", "punta pie izq.", "punta pie der."]
# Pares de puntos que se unen con una línea (esqueleto)
CONEXIONES = [(0, 1), (1, 2), (2, 3), (3, 7), (0, 4), (4, 5), (5, 6), (6, 8), (9, 10), (11, 12), (11, 13), (13, 15),
              (15, 17), (15, 19), (15, 21), (17, 19), (12, 14), (14, 16), (16, 18), (16, 20), (16, 22), (18, 20),
              (11, 23), (12, 24), (23, 24), (23, 25), (24, 26), (25, 27), (26, 28), (27, 29), (28, 30), (29, 31),
              (30, 32), (27, 31), (28, 32)]


def abrir(texto):
    """Convierte el texto Base64 (con o sin el envoltorio b'...') en una imagen."""
    texto = texto.strip()
    if texto[:2] in ("b'", 'b"'):
        texto = texto[2:-1]
    return Image.open(io.BytesIO(base64.b64decode(texto)))


def puntos(texto):
    """33 filas (x, y, z, visibility) a partir del texto de poseLandmarks."""
    return np.array(NUMERO.findall(texto), dtype=float).reshape(33, 4)


df = pd.read_csv(CSV)
con = df[df["poseLandmarks"].notna()]

# Comprobación del orden de los puntos con todos los registros que tienen pose
P = np.array([puntos(s) for s in con["poseLandmarks"]])
print("registros con pose:", P.shape[0], "| puntos:", P.shape[1], "| valores por punto:", P.shape[2])
for i in (0, 11, 12, 15, 16, 23, 24, 25, 26, 27, 28, 31, 32):
    print(f"  {i:2d} {NOMBRES[i]:16s} y media = {P[:, i, 1].mean():.2f}")
print("x: min %.2f máx %.2f | y: min %.2f máx %.2f | visibility: min %.2f máx %.2f"
      % (P[:, :, 0].min(), P[:, :, 0].max(), P[:, :, 1].min(), P[:, :, 1].max(), P[:, :, 3].min(), P[:, :, 3].max()))

# Dibujo sobre la primera imagen de sentadilla (se usa el tamaño REAL de la imagen, no el declarado)
fila = con[con["excercise"].str.strip().str.lower() == "squat"].iloc[0]
img = abrir(fila["base64encoded"]).convert("RGB")
ancho, alto = img.size
pts = puntos(fila["poseLandmarks"])
print("imagen real:", ancho, "x", alto, "| declarada:", fila["imageWidth"], "x", fila["imageHeight"])

fig, ax = plt.subplots(figsize=(7, 7 * alto / ancho + 0.4), dpi=130)
ax.imshow(img)
for a, b in CONEXIONES:
    ax.plot([pts[a, 0] * ancho, pts[b, 0] * ancho], [pts[a, 1] * alto, pts[b, 1] * alto], color="#00c2ff", lw=2, zorder=2)
ax.scatter(pts[:, 0] * ancho, pts[:, 1] * alto, s=70, c="#ffd400", edgecolors="#222", linewidths=0.8, zorder=3)
for i in range(33):
    ax.text(pts[i, 0] * ancho, pts[i, 1] * alto, str(i), fontsize=6.5, ha="center", va="center",
            color="#111", zorder=4, fontweight="bold")
ax.set_axis_off()
ax.set_title("Los 33 puntos de poseLandmarks sobre una imagen del dataset (squat)", fontsize=9)
fig.tight_layout()
fig.savefig(SALIDA)
print("guardado:", SALIDA)
