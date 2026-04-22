import os
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

# Importamos nuestro nuevo sistema hipervitaminado de logs
from utils.logs.logging import logger

load_dotenv()

class PostgresConnector:
    """
    Conector dinámico y estandarizado para ejecutar SQL en múltiples orígenes.
    """
    
    def __init__(self, db_url_env_var: str, dbname: str = "postgres"):
        """
        :param db_url_env_var: Variable en el .env con la URL raíz.
                               Ej: 'postgresql://user:pass@192.168.0.1:5432'
        :param dbname: Nombre de la base de datos a la cual conectarse. (Por defecto: 'postgres')
        """
        self.db_url_env_var = db_url_env_var
        self.dbname = dbname
        base_url = os.getenv(db_url_env_var)
        
        if not base_url:
            raise ValueError(f"La variable de entorno '{db_url_env_var}' no está definida o está vacía en el .env")
            
        # Normalizamos la URL base. Le quitamos el slash final si el usuario lo puso por error
        if base_url.endswith("/"):
            base_url = base_url[:-1]
            
        # Unimos el servidor base con la base de datos objetivo
        self.db_url = f"{base_url}/{self.dbname}"
        self.conn = None

    def _get_connection(self):
        """Maneja la creación y retorno de la conexión de forma segura."""
        if self.conn is None or self.conn.closed:
            try:
                self.conn = psycopg.connect(self.db_url)
                logger.success(f"Conexión exitosa a la base de datos '{self.dbname}' del servidor '{self.db_url_env_var}'")
            except Exception as e:
                logger.exception(f"Error crítico conectando a la base de datos '{self.dbname}' en '{self.db_url_env_var}'")
                raise
        return self.conn

    def execute_query(self, query: str, params: tuple = None) -> list:
        """
        Ejecuta consultas de tipo SELECT y retorna los resultados como una lista de diccionarios.
        
        :param query: Sentencia SQL SELECT.
        :param params: Tupla de parámetros para evitar inyección SQL (opcional).
        :return: Lista de diccionarios con la forma [{'columna': 'valor'}, ...]
        """
        conn = self._get_connection()
        try:
            # Para psycopg3, se pasa row_factory en vez de cursor_factory
            with conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        except Exception as e:
            logger.exception(f"Error ejecutando consulta SELECT en la base de datos '{self.db_url_env_var}'")
            raise

    def execute_transaction(self, query: str, params: tuple = None) -> int:
        """
        Ejecuta consultas INSERT, UPDATE, DELETE o ejecuciones por lotes (DDL/DML).
        Maneja commit y rollback de forma automática.
        
        :param query: Sentencia SQL (INSERT, UPDATE, DELETE, etc.).
        :param params: Tupla o lista de parámetros.
        :return: Entero indicando el número de filas afectadas (rowcount).
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                affected_rows = cursor.rowcount
            conn.commit()
            return affected_rows
        except Exception as e:
            conn.rollback()
            logger.exception(f"Error ejecutando consulta transaccional (INSERT/UPDATE/DELETE) en '{self.db_url_env_var}'")
            raise

    def close(self):
        """Cierra la conexión explícitamente si está abierta."""
        if self.conn is not None and not self.conn.closed:
            self.conn.close()
            logger.info(f"Conexión con '{self.db_url_env_var}' cerrada exitosamente.")

    # --- Soporte para Context Managers (Uso con bloque 'with') ---
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
