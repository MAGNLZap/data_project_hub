import os
import sqlite3
from dotenv import load_dotenv

# Importamos nuestro sistema centralizado de logs
from utils.logs.logging import logger

load_dotenv()

class SqliteConnector:
    """
    Conector dinámico y estandarizado para ejecutar SQL en bases de datos SQLite locales.
    """
    
    def __init__(self, db_path_env_var: str):
        """
        :param db_path_env_var: El nombre de la variable de entorno que guarda la ruta al archivo
                              Ejemplo en .env: DB_SQLITE_LOCAL="data/output/mi_base.db"
        """
        self.db_path_env_var = db_path_env_var
        self.db_filepath = os.getenv(db_path_env_var)
        
        if not self.db_filepath:
            raise ValueError(f"La variable de entorno '{db_path_env_var}' no está definida en el .env")
            
        self.conn = None

    def _get_connection(self):
        """Maneja la creación de la conexión de modo seguro hacia el archivo local"""
        if self.conn is None:
            try:
                # El parámetro check_same_thread=False avisa que puedes compartir esto en hilos (opcional)
                self.conn = sqlite3.connect(self.db_filepath, check_same_thread=False)
                
                # Configurar que el motor devuelva diccionarios en lugar de tuplas numéricas planas (Como el dict_row)
                self.conn.row_factory = sqlite3.Row
                
                logger.success(f"Conexión exitosa al archivo SQLite usando variable '{self.db_path_env_var}'")
            except Exception as e:
                logger.exception(f"Error crítico conectando o creando el archivo SQLite en '{self.db_filepath}'")
                raise
        return self.conn

    def execute_query(self, query: str, params: tuple = ()) -> list:
        """
        Ejecuta consultas de tipo SELECT y retorna los resultados como una lista de diccionarios.
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            cursor.close()
            # Convertimos nativamente los rows en diccionarios reales antes de devolverlos
            return [dict(row) for row in rows]
        except Exception as e:
            logger.exception(f"Error ejecutando consulta SELECT en SQLite '{self.db_path_env_var}'")
            raise

    def execute_transaction(self, query: str, params: tuple = ()) -> int:
        """
        Ejecuta consultas INSERT, UPDATE, DELETE o ejecuciones DDL.
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            affected_rows = cursor.rowcount
            conn.commit()
            cursor.close()
            return affected_rows
        except Exception as e:
            conn.rollback()
            logger.exception(f"Error ejecutando consulta transaccional en SQLite '{self.db_path_env_var}'")
            raise

    def close(self):
        """Cierra la conexión al archivo."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            logger.info(f"Conexión SQLite con '{self.db_path_env_var}' cerrada exitosamente.")

    # --- Soporte para Context Managers (bloque 'with') ---
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
