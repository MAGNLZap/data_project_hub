import os
import pyodbc
from dotenv import load_dotenv

# Importamos el logger general
from utils.logs.logging import logger

load_dotenv()

class SqlServerConnector:
    """
    Conector dinámico y estandarizado para ejecutar SQL en orígenes MS SQL Server 
    utilizando ODBC.
    """
    
    def __init__(self, db_url_env_var: str, dbname: str = "master"):
        """
        :param db_url_env_var: Variable en el .env con todas las credenciales de host.
                               Ej: 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=IP;UID=user;PWD=pass'
        :param dbname: Nombre de la base de datos a conectar (Por defecto: 'master')
        """
        self.db_url_env_var = db_url_env_var
        self.dbname = dbname
        base_connection_string = os.getenv(db_url_env_var)
        
        if not base_connection_string:
            raise ValueError(f"La variable de entorno '{db_url_env_var}' no está definida o está vacía en el .env")
            
        # SQLServer permite agregar la bd al final de la cadena de texto simplemente apilándola
        self.connection_string = f"{base_connection_string};DATABASE={self.dbname}"
        self.conn = None

    def _get_connection(self):
        """Maneja la creación y retorno de la conexión de modo seguro hacia SQL Server."""
        if self.conn is None:
            try:
                # Nos conectamos y apagamos el autocommit para mantener los estándares transaccionales (ACID)
                self.conn = pyodbc.connect(self.connection_string, autocommit=False)
                logger.success(f"Conexión exitosa a la base '{self.dbname}' en SQL Server desde '{self.db_url_env_var}'")
            except Exception as e:
                logger.exception(f"Error crítico conectando a la base '{self.dbname}' en SQL Server")
                raise
        return self.conn

    def execute_query(self, query: str, params: tuple = ()) -> list:
        """
        Ejecuta consultas de tipo SELECT. PyODBC por defecto devuelve tuplas, 
        aquí lo forzamos automáticamente a "lista de diccionarios" igual que los otros conectores.
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            
            # Extraemos los nombres reales de las columnas en SQL Server
            columns = [column[0] for column in cursor.description]
            
            # Pegamos la columna con su valor usando zip y convertimos cada fila a dict
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            cursor.close()
            return results
        except Exception as e:
            logger.exception(f"Error ejecutando consulta SELECT en SQL Server '{self.db_url_env_var}'")
            raise

    def execute_transaction(self, query: str, params: tuple = ()) -> int:
        """
        Ejecuta consultas transaccionales (DML: INSERT, UPDATE, DELETE).
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
            logger.exception(f"Error en consulta transaccional de SQL Server '{self.db_url_env_var}'")
            raise

    def close(self):
        """Cierra el nodo explícitamente."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            logger.info(f"Conexión MS SQL Server con '{self.db_url_env_var}' cerrada estandarizadamente.")

    # --- Soporte Context Manager (Pattern de la casa) ---
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
