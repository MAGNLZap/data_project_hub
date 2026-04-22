import os
import pandas as pd
from utils.logs.logging import logger

def write_to_csv(data, filename: str, sep: str = ',', encoding: str = 'utf-8') -> str:
    """
    Recibe un Pandas DataFrame o una Lista de Diccionarios y lo escribe
    estandarizadamente en un archivo CSV en la carpeta /data/output.
    """
    
    # Si le envías una Lista de Diccionarios de una Base de Datos, lo convierte
    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, pd.DataFrame):
        df = data
    else:
        error_msg = f"Estructura no soportada. El CSV Writer espera un DataFrame o una List[dict]. Tipo recibido: {type(data)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    if df.empty:
        logger.warning(f"Se intentó escribir '{filename}' pero la data origen está completamente vacía (0 registros).")
        
    # Calcular la ruta Output dinámica (Sube 3 niveles: writers -> utils -> Raíz -> data/output)
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    output_folder = os.path.join(base_dir, 'data', 'output')
    
    # Estándares de seguridad por si la carpeta no existe en tu repositorio
    os.makedirs(output_folder, exist_ok=True)
    
    file_path = os.path.join(output_folder, filename)
    
    try:
        # Volcado a disco
        df.to_csv(file_path, sep=sep, index=False, encoding=encoding)
        logger.success(f"Archivo plano exportado exitosamente en '{file_path}' ({len(df)} registros)")
        return file_path
    except Exception as e:
        logger.exception(f"Error mortal exportando Data a archivo plano CSV '{filename}'")
        raise
