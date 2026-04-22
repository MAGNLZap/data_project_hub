import os
import re
import csv
import pandas as pd
import numpy as np
from utils.logs.logging import logger

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Homologa cabeceras a minúsculas, usando snake_case."""
    cleaned_columns = []
    for col in df.columns:
        col_name = str(col).strip().lower().replace(' ', '_')
        col_name = re.sub(r'[^a-z0-9_]', '', col_name)
        cleaned_columns.append(col_name)
        
    df.columns = cleaned_columns
    return df

def detect_csv_separator(file_path: str, encoding: str = 'utf-8') -> str:
    """Implementa un Sniffer para oler y deducir la sintaxis interna del CSV."""
    try:
        with open(file_path, 'r', encoding=encoding) as file:
            # Solo leemos los primeros 2048 bytes para no agotar la RAM
            sample = file.read(2048)
            # Sniffer inspecciona ese extracto para adivinar si usa ',' o ';' o '\t'
            dialect = csv.Sniffer().sniff(sample)
            return dialect.delimiter
    except Exception as e:
        logger.warning(f"No se pudo inferir automáticamente el separador. Se usará la coma por defecto: {e}")
        return ','

def read_csv_normalized(filename: str, sep=None, encoding='utf-8', parse_dates=False) -> pd.DataFrame:
    """
    Lee un archivo CSV auto-determinando su separador si es requerido, 
    y retorna un DataFrame purificado (limpio de basura, tipado y estandarizado).
    """
    # 1. Resolución Automática de Ruta 
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    rutas_candidatas = [
        os.path.join(base_dir, 'data', 'input', filename),
        os.path.join(base_dir, 'data', 'output', filename),
        os.path.join(base_dir, 'data', 'sandbox', filename)
    ]
    
    file_path = None
    for ruta in rutas_candidatas:
        if os.path.isfile(ruta):
            file_path = ruta
            logger.info(f"Archivo CSV '{filename}' detectado en {os.path.basename(os.path.dirname(ruta))}")
            break
            
    if not file_path:
        error_msg = f"No se encontró el archivo CSV '{filename}' en input, output ni sandbox."
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    # 2. Análisis del motor (Identificar Separador Automático si no se provee)
    if not sep:
        sep = detect_csv_separator(file_path, encoding)
        logger.info(f"Separador inferido para '{filename}': [ {sep} ]")

    # 3. Extracción Cruda
    try:
        df = pd.read_csv(file_path, sep=sep, encoding=encoding, parse_dates=parse_dates, engine='python')
    except UnicodeDecodeError:
        logger.warning(f"Fallo codificación UTF-8 en '{filename}'. Se rotará al charset latino (latin1).")
        encoding = 'latin1'
        df = pd.read_csv(file_path, sep=sep, encoding=encoding, parse_dates=parse_dates, engine='python')
    except Exception as e:
        logger.exception(f"Error mortal parseando el archivo CSV '{filename}'")
        raise
        
    # 4. Limpieza Estructural Profunda (Filtros Anti-Basura)
    df = df.dropna(how='all', axis=0) # Borrar registros o filas vacías completas
    df = df.dropna(how='all', axis=1) # Borrar columnas vacías completas
    df = clean_column_names(df)
    
    # 5. Higiene de Strings Internos
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: str(x).strip() if pd.notnull(x) and isinstance(x, str) else x)
            df[col] = df[col].replace(r'^\s*$', np.nan, regex=True)
            df[col] = df[col].where(pd.notnull(df[col]), None)

    logger.success(f"CSV normalizado. Output: {len(df)} filas.")
    return df
