import os
import re
import pandas as pd
import numpy as np
from utils.logs.logging import logger

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza los nombres de las columnas para compatibilidad universal en bases de datos"""
    cleaned_columns = []
    for col in df.columns:
        col_name = str(col)
        col_name = col_name.strip().lower()
        col_name = col_name.replace(' ', '_')
        col_name = re.sub(r'[^a-z0-9_]', '', col_name)
        cleaned_columns.append(col_name)
        
    df.columns = cleaned_columns
    return df

def read_excel_normalized(filename: str, sheet_name=0, dtype=None, parse_dates=False) -> pd.DataFrame:
    """
    Lee un archivo Excel y devuelve un DataFrame completamente estandarizado y limpiado.
    Resolución de ruta automática buscando en data/input o data/output de la raíz.
    """
    
    # 1. Resolución Automática de Ruta (Con 3 saltos para salir de utils/readers/)
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    ruta_input = os.path.join(base_dir, 'data', 'input', filename)
    ruta_output = os.path.join(base_dir, 'data', 'output', filename)
    ruta_sandbox = os.path.join(base_dir, 'data', 'sandbox', filename)
    
    file_path = None
    if os.path.isfile(ruta_input):
        file_path = ruta_input
        logger.info(f"Archivo Excel '{filename}' localizado en data/input")
    elif os.path.isfile(ruta_output):
        file_path = ruta_output
        logger.info(f"Archivo Excel '{filename}' localizado en data/output")
    elif os.path.isfile(ruta_sandbox):
        file_path = ruta_sandbox
        logger.info(f"Archivo Excel '{filename}' localizado en data/sandbox")
    else:
        error_msg = f"No se encontró el archivo '{filename}'. Se buscó en 'data/input' y 'data/output' desde la raíz del proyecto."
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    # 2. Extracción Bruta usando Pandas
    try:
        df = pd.read_excel(
            file_path, 
            sheet_name=sheet_name, 
            dtype=dtype, 
            parse_dates=parse_dates
        )
    except Exception as e:
        logger.exception(f"Error parseando el archivo Excel '{filename}'")
        raise
        
    # 3. Limpieza Estructural Profunda
    df = df.dropna(how='all', axis=0) # Filas basuras enteras
    df = df.dropna(how='all', axis=1) # Columnas basuras enteras
    
    df = clean_column_names(df)
    
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: str(x).strip() if pd.notnull(x) and isinstance(x, str) else x)
            df[col] = df[col].replace(r'^\s*$', np.nan, regex=True)
            df[col] = df[col].where(pd.notnull(df[col]), None)

    logger.success(f"Extracción y Normalización de '{filename}' completada. {len(df)} registros devueltos.")
    return df
