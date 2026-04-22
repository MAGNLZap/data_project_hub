import os
import sys
from loguru import logger

# 1. Definir rutas basándonos en la raíz de tu proyecto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, "logs")

# Aseguramos que la carpeta logs exista físicamente en tu proyecto
os.makedirs(LOG_DIR, exist_ok=True)

# 2. Limpiar la configuración por defecto de loguru (para evitar duplicados y controlarlo nosotros)
logger.remove()

# --- CANAL 1: (ELIMINADO) No arrojaremos logs por consola ---
# Ya que se utiliza la librería "rich" a nivel de usuario interactivo,
# hacemos que Loguru actúe de modo silencioso (Background) únicamente hacia los archivos.


# --- CANAL 2: Trazabilidad general en archivos rotativos ---
# Excluye explícitamente los mensajes de tipo ERROR o CRITICAL (level < 40).
logger.add(
    os.path.join(LOG_DIR, "data_hub_{time:YYYY-MM-DD}.log"),
    rotation="10 MB",
    retention="15 days",
    level="DEBUG",
    filter=lambda record: record["level"].no < logger.level("ERROR").no,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    encoding="utf-8"
)

# --- CANAL 3: Archivos de Errores ---
# Sólo guarda eventos de nivel ERROR o CRITICAL.
# 'backtrace' y 'diagnose' sacan "una radiografía" a tu entorno: muestran los valores exactos
# de las variables que causaron que el programa o la conexión SQL colapsara.
logger.add(
    os.path.join(LOG_DIR, "errors.log"),
    rotation="5 MB",
    retention="30 days", 
    level="ERROR",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    backtrace=True, 
    diagnose=True,
    encoding="utf-8"
)

# Exportamos el objeto logger ya calibrado para que se use en todo el proyecto
__all__ = ["logger"]
