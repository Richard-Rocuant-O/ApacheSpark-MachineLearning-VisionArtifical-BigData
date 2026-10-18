"""Genera los diagramas de arquitectura del informe (Figuras 3.1 y 3.2) como SVG.

Uso:  python generar_diagramas.py
Salida: arquitectura_implementada.svg y arquitectura_ha.svg, junto a este archivo.
Los iconos son glifos simples propios; el color indica la categoría del componente.
"""
from html import escape
from pathlib import Path

AQUI = Path(__file__).resolve().parent

# Colores por categoría de componente
C_COMPUTO = "#ED7100"
C_ALMACEN = "#3F8624"
C_BD = "#3B48CC"
C_INTEGRA = "#E7157B"
C_ANALISIS = "#8C4FFF"
C_SEGUR = "#DD344C"
C_FUENTE = "#5A6C86"
TINTA = "#232f3e"
GRIS = "#4d5b6e"

# Glifos blancos dibujados en una cuadrícula de 48 x 48
W = 'stroke="#fff" stroke-width="2.4" fill="none" stroke-linecap="round" stroke-linejoin="round"'


def _chip():
    pines = "".join(
        f'<path d="M{p} 9V15M{p} 33V39M9 {p}H15M33 {p}H39" {W}/>' for p in (20, 24, 28)
    )
    return (f'<rect x="15" y="15" width="18" height="18" rx="2" {W}/>'
            '<rect x="21" y="21" width="6" height="6" fill="#fff"/>' + pines)


GLIFOS = {
    "chip": _chip(),
    "bd": (f'<ellipse cx="24" cy="14" rx="11" ry="4" {W}/>'
           f'<path d="M13 14V34A11 4 0 0 0 35 34V14M13 24A11 4 0 0 0 35 24" {W}/>'),
    "balde": (f'<ellipse cx="24" cy="15" rx="12" ry="4" {W}/>'
              f'<path d="M12 15L15 36A9 3 0 0 0 33 36L36 15M13.5 25A10.5 3 0 0 0 34.5 25" {W}/>'),
    "parquet": (f'<rect x="12" y="11" width="24" height="26" rx="2" {W}/>'
                f'<path d="M20 11V37M28 11V37M12 19H36" {W}/>'),
    "cola": ('<rect x="7" y="19" width="9" height="10" fill="#fff"/>'
             '<rect x="19.5" y="19" width="9" height="10" fill="#fff"/>'
             '<rect x="32" y="19" width="9" height="10" fill="#fff"/>'),
    "orquestador": (f'<path d="M24 12V22M24 26L12 36M24 26V36M24 26L36 36" {W}/>'
                    '<circle cx="24" cy="24" r="4" fill="#fff"/>'
                    '<circle cx="12" cy="37" r="3" fill="#fff"/><circle cx="24" cy="37" r="3" fill="#fff"/>'
                    '<circle cx="36" cy="37" r="3" fill="#fff"/><circle cx="24" cy="11" r="3" fill="#fff"/>'),
    "ml": (f'<path d="M13 15L24 24L35 15M13 33L24 24L35 33M13 15V33M35 15V33" {W}/>'
           '<circle cx="13" cy="15" r="3.5" fill="#fff"/><circle cx="35" cy="15" r="3.5" fill="#fff"/>'
           '<circle cx="13" cy="33" r="3.5" fill="#fff"/><circle cx="35" cy="33" r="3.5" fill="#fff"/>'
           '<circle cx="24" cy="24" r="4.5" fill="#fff"/>'),
    "barras": (f'<path d="M10 38H38" {W}/>'
               '<rect x="12" y="26" width="6" height="10" fill="#fff"/>'
               '<rect x="21" y="18" width="6" height="18" fill="#fff"/>'
               '<rect x="30" y="10" width="6" height="26" fill="#fff"/>'),
    "campana": (f'<path d="M15 32V22A9 9 0 0 1 33 22V32L36 35H12Z" {W}/>'
                f'<path d="M21 39A3 3 0 0 0 27 39" {W}/>'),
    "llave": (f'<circle cx="16" cy="24" r="6" {W}/>'
              f'<path d="M22 24H38M32 24V30M37 24V29" {W}/>'),
    "nube": (f'<path d="M15 34A7 7 0 0 1 14 20A9.5 9.5 0 0 1 32 17A8 8 0 0 1 34 34Z" {W}/>'),
    "descarga": (f'<path d="M24 10V28M17 21L24 28L31 21M11 35H37" {W}/>'),
}


class Lienzo:
    def __init__(self, ancho, alto):
        self.ancho, self.alto = ancho, alto
        self.partes = []

    def agregar(self, svg):
        self.partes.append(svg)

    # --- texto -------------------------------------------------------------
    def texto(self, x, y, lineas, tam=13, ancla="middle", peso="400", color=TINTA, interlineado=None, halo=False):
        if isinstance(lineas, str):
            lineas = [lineas]
        lh = interlineado or tam * 1.28
        extra = ' paint-order="stroke" stroke="#fff" stroke-width="5" stroke-linejoin="round"' if halo else ""
        tspans = "".join(
            f'<tspan x="{x}" dy="{0 if i == 0 else lh:.1f}">{escape(t)}</tspan>' for i, t in enumerate(lineas)
        )
        self.agregar(
            f'<text x="{x}" y="{y}" font-size="{tam}" text-anchor="{ancla}" font-weight="{peso}" '
            f'fill="{color}"{extra}>{tspans}</text>'
        )

    # --- contenedores ------------------------------------------------------
    def contenedor(self, x, y, w, h, rotulo, estilo="sólido", relleno="none", color=TINTA, tam=13.5, rotulo_y=None):
        dash = {"sólido": "", "guiones": ' stroke-dasharray="7 5"'}[estilo]
        self.agregar(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="6" fill="{relleno}" '
            f'stroke="{color}" stroke-width="1.6"{dash}/>'
        )
        if rotulo:
            self.texto(x + w / 2, rotulo_y or (y + h - 12), rotulo, tam=tam, color=color if color != "#ED7100" else "#b55400")

    # --- iconos ------------------------------------------------------------
    def icono(self, cx, cy, glifo, color, rotulo=None, insignia=None, tam=56, tam_rotulo=12.5):
        x, y = cx - tam / 2, cy - tam / 2
        esc = tam / 48
        self.agregar(
            f'<g transform="translate({x},{y}) scale({esc})">'
            f'<rect width="48" height="48" rx="6" fill="{color}"/>{GLIFOS[glifo]}</g>'
        )
        if insignia:
            self.insignia(x + tam, y, insignia)
        if rotulo:
            self.texto(cx, y + tam + 17, rotulo, tam=tam_rotulo)

    def insignia(self, x, y, numero):
        self.agregar(f'<circle cx="{x}" cy="{y}" r="11" fill="{TINTA}" stroke="#fff" stroke-width="1.5"/>')
        self.texto(x, y + 4.5, str(numero), tam=12.5, peso="700", color="#fff")

    # --- flechas -----------------------------------------------------------
    def flecha(self, puntos, rotulo=None, pos_rotulo=None, punteada=False, doble=False, color=GRIS, ancla="middle"):
        d = "M" + " L".join(f"{x},{y}" for x, y in puntos)
        dash = ' stroke-dasharray="3 4"' if punteada else ""
        ini = ' marker-start="url(#punta-ini)"' if doble else ""
        self.agregar(
            f'<path d="{d}" fill="none" stroke="{color}" stroke-width="1.8"{dash}'
            f'{ini} marker-end="url(#punta)"/>'
        )
        if rotulo:
            x, y = pos_rotulo
            self.texto(x, y, rotulo, tam=11.5, color=GRIS, halo=True, ancla=ancla)

    # --- leyendas ----------------------------------------------------------
    def leyenda(self, x, y, elementos_color, capas=True):
        cursor = x
        if capas:
            for n, nombre in ((1, "Ingesta"), (2, "Almacenamiento"), (3, "Procesamiento"), (4, "Análisis y modelamiento")):
                self.insignia(cursor + 11, y, n)
                self.texto(cursor + 28, y + 4.5, nombre, tam=12.5, ancla="start")
                cursor += 28 + len(nombre) * 7.2 + 26
            cursor += 24
        for color, nombre in elementos_color:
            self.agregar(f'<rect x="{cursor}" y="{y - 8}" width="16" height="16" rx="3" fill="{color}"/>')
            self.texto(cursor + 24, y + 4.5, nombre, tam=12.5, ancla="start")
            cursor += 24 + len(nombre) * 7 + 22

    def svg(self):
        defs = (
            '<defs>'
            f'<marker id="punta" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto">'
            f'<path d="M0 0L10 5L0 10Z" fill="{GRIS}"/></marker>'
            f'<marker id="punta-ini" viewBox="0 0 10 10" refX="1" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse">'
            f'<path d="M0 0L10 5L0 10Z" fill="{GRIS}"/></marker>'
            '</defs>'
        )
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.ancho}" height="{self.alto}" '
            f'viewBox="0 0 {self.ancho} {self.alto}" font-family="Segoe UI, Arial, Helvetica, sans-serif">'
            f'{defs}<rect width="{self.ancho}" height="{self.alto}" fill="#fff"/>' + "".join(self.partes) + "</svg>"
        )


# =============================================================================
# Figura 3.1  Arquitectura implementada
# =============================================================================
def arquitectura_implementada():
    L = Lienzo(1600, 740)

    # Fuente externa
    L.icono(110, 138, "nube", C_FUENTE, ["Kaggle", "imagePoses.csv", "3.501 registros"])

    # Google Colab
    L.contenedor(230, 40, 800, 620, "Google Colab (sesión gratuita, 2 núcleos)", "guiones", color="#ED7100", relleno="#fffaf5")
    L.icono(330, 138, "descarga", C_INTEGRA, ["KaggleHub o carga", "manual (notebook 01)"], insignia=1)
    L.icono(590, 138, "chip", C_COMPUTO, ["PySpark y pandas/PyArrow:", "validación, depuración", "y transformación"], insignia=3)
    L.icono(850, 138, "parquet", C_ALMACEN, ["Parquet + Snappy", "particionado por ejercicio", "y ZIP del dataset"], insignia=2)
    L.icono(850, 338, "ml", C_ANALISIS, ["Spark MLlib: Random Forest,", "regresión logística y", "validación cruzada"], insignia=4)
    L.icono(330, 538, "llave", C_SEGUR, ["Secretos de Colab", "(SUPABASE_DB_URL)"])
    L.icono(590, 538, "chip", C_COMPUTO, ["Resumen por ejercicio", "(Spark SQL + psycopg)"], insignia=3)

    # Databricks
    L.contenedor(1110, 40, 460, 290, "Databricks Free Edition (serverless)", "sólido")
    L.icono(1210, 138, "balde", C_ALMACEN, ["Volume de", "Unity Catalog"], insignia=2)
    L.icono(1470, 138, "chip", C_COMPUTO, ["Spark: lectura y", "validación (notebook 03)"], insignia=3)

    # Supabase
    L.contenedor(1110, 390, 460, 270, "Supabase (PostgreSQL gestionado)", "sólido")
    L.icono(1340, 520, "bd", C_BD, ["Tabla resumen_ejercicios", "seguridad por filas (RLS)"], insignia=2)

    # Flechas
    L.flecha([(150, 138), (298, 138)], "descarga", (190, 124))
    L.flecha([(362, 138), (558, 138)], "CSV en raw/", (460, 124))
    L.flecha([(622, 138), (818, 138)], "escritura con PyArrow", (720, 124))
    L.flecha([(850, 252), (850, 306)], "ZIP", (868, 284), ancla="start")
    L.flecha([(590, 238), (590, 506)], "DataFrame", (608, 372), ancla="start")
    L.flecha([(362, 538), (558, 538)], "URI de conexión", (460, 524))
    L.flecha([(622, 538), (1308, 538)], "INSERT … ON CONFLICT, conexión cifrada (SSL)", (830, 524))
    L.flecha([(882, 138), (1178, 138)], "ZIP (carga manual)", (962, 124))
    L.flecha([(1242, 138), (1438, 138)], "lectura", (1340, 124))

    # Leyenda
    L.leyenda(60, 706, [(C_COMPUTO, "Cómputo"), (C_ALMACEN, "Almacenamiento"), (C_BD, "Base de datos"),
                        (C_ANALISIS, "Análisis"), (C_SEGUR, "Seguridad"), (C_INTEGRA, "Ingesta")])
    return L.svg()


# =============================================================================
# Figura 3.2  Diseño objetivo con alta disponibilidad
# =============================================================================
def arquitectura_ha():
    L = Lienzo(1700, 1010)

    # Fuentes externas
    L.icono(80, 420, "nube", C_FUENTE, ["Fuentes de datos", "(archivos, APIs,", "eventos)"])

    # Región
    L.contenedor(210, 30, 1260, 920, "Región de la nube (diseño objetivo, no desplegado)", "sólido", tam=14, rotulo_y=935)

    # Ingesta
    L.contenedor(240, 190, 190, 470, "Ingesta", "sólido", relleno="#fafbfc")
    L.icono(335, 262, "cola", C_INTEGRA, ["Cola de mensajes", "(streaming, con", "reintentos)"], insignia=1)
    L.icono(335, 480, "descarga", C_INTEGRA, ["Carga por lotes", "(idempotente)"], insignia=1)

    # Orquestador
    L.icono(785, 100, "orquestador", C_ANALISIS, None)
    L.texto(785, 150, ["Driver y orquestador (reintentos y", "reinicio automático)"], tam=12.5)

    # Zonas de disponibilidad A y B
    for nombre, x, cx, bd_rotulo, bd_color in (
        ("Zona de disponibilidad A", 470, 605, ["PostgreSQL primario", "(escritura)"], C_BD),
        ("Zona de disponibilidad B", 830, 965, ["PostgreSQL réplica", "(lectura)"], C_BD),
    ):
        L.contenedor(x, 195, 270, 505, nombre, "guiones", color="#ED7100", relleno="#fffaf5")
        L.contenedor(x + 20, 225, 230, 150, "Grupo de autoescalado", "guiones", color=TINTA, relleno="#fff", tam=12.5)
        L.icono(cx, 275, "chip", C_COMPUTO, "Nodo Spark (executor)", insignia=3)
        L.contenedor(x + 20, 410, 230, 230, "Subred de datos (privada)", "sólido", relleno="#fff", tam=12.5)
        L.icono(cx, 485, "bd", bd_color, bd_rotulo, insignia=2)

    # Data Lake
    L.icono(785, 790, "balde", C_ALMACEN, None, insignia=2)
    L.texto(785, 850, ["Data Lake: almacenamiento de objetos", "replicado entre zonas (Parquet)"], tam=12.5)

    # Análisis y consumo
    L.icono(1290, 262, "ml", C_ANALISIS, ["Modelos versionados y", "reentrenamiento orquestado"], insignia=4)
    L.icono(1290, 485, "barras", C_ANALISIS, ["Paneles de BI", "sobre el warehouse"], insignia=4)

    # Servicios transversales
    L.contenedor(1170, 600, 240, 300, "Servicios transversales", "guiones", color=GRIS, relleno="#fafbfc", tam=12.5)
    L.icono(1290, 665, "campana", C_SEGUR, ["Monitoreo y alertas"])
    L.icono(1290, 790, "llave", C_SEGUR, ["Gestor de secretos"])

    # Flechas
    L.flecha([(112, 420), (160, 420), (160, 262), (303, 262)])
    L.flecha([(160, 420), (160, 480), (303, 480)])
    L.flecha([(367, 262), (488, 262)])
    L.flecha([(757, 100), (605, 100), (605, 223)])
    L.flecha([(813, 100), (965, 100), (965, 223)])
    L.flecha([(605, 375), (605, 453)], "lectura y escritura", (623, 398), ancla="start")
    L.flecha([(965, 375), (965, 453)], "consultas", (983, 398), ancla="start")
    L.flecha([(633, 485), (937, 485)], "replicación", (785, 470), punteada=True)
    L.flecha([(367, 480), (400, 480), (400, 790), (751, 790)], "carga de archivos", (575, 808))
    L.flecha([(520, 375), (520, 770), (751, 770)], "lectura y escritura", (635, 757), doble=True)
    L.flecha([(1050, 375), (1050, 770), (819, 770)], "lectura y escritura", (935, 757), doble=True)
    L.flecha([(1080, 262), (1258, 262)], "entrenamiento", (1180, 246))
    L.flecha([(993, 485), (1258, 485)], "consultas SQL", (1180, 470))

    L.leyenda(60, 985, [(C_COMPUTO, "Cómputo"), (C_ALMACEN, "Almacenamiento"), (C_BD, "Base de datos"),
                        (C_ANALISIS, "Análisis"), (C_SEGUR, "Operación y seguridad"), (C_INTEGRA, "Integración")])
    return L.svg()


if __name__ == "__main__":
    (AQUI / "arquitectura_implementada.svg").write_text(arquitectura_implementada(), encoding="utf-8")
    (AQUI / "arquitectura_ha.svg").write_text(arquitectura_ha(), encoding="utf-8")
    print("SVG generados en", AQUI)
