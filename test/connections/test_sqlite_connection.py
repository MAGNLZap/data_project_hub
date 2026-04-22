import os
import sys
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connections.sqlite import SqliteConnector
from utils.logs.logging import logger

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import print as rprint

def run_test(env_var_to_test):
    console = Console()
    
    console.print(Panel.fit(
        f"🎯 [bold green]Prueba de Conexión SQLite[/bold green]\n"
        f"Archivo alojado en variable: [bold yellow]{env_var_to_test}[/bold yellow]",
        border_style="green"
    ))

    logger.info(f"Test interactivo hacia SQLite mediante la variable de entorno: '{env_var_to_test}'")

    try:
        with console.status(f"[bold yellow]Conectando el archivo local...", spinner="material") as status:
            with SqliteConnector(env_var_to_test) as db_conn:
                
                # Vamos a crear una tabla temporal en memoria solo para validar permisos de escritura
                query_create = "CREATE TABLE IF NOT EXISTS _test_hub (id INTEGER PRIMARY KEY, valor TEXT)"
                db_conn.execute_transaction(query_create)
                
                # Simulamos ejecutar un query nativo de SQLite
                query = "SELECT 1 as control_test, datetime('now') as server_time"
                result = db_conn.execute_query(query)

        table = Table(show_header=True, header_style="bold cyan", border_style="cyan")
        table.add_column("Columna", style="dim", width=20)
        table.add_column("Valor extraído", style="bold green")

        for row in result:
            table.add_row("control_test", str(row.get('control_test')))
            table.add_row("server_time", str(row.get('server_time')))

        console.print(table)
        console.print(Panel(f"✅ [bold green]Tu conexión SQLite funciona perfectamente.[/bold green]", border_style="green"))
        
        logger.success(f"La validación completa hacia '{env_var_to_test}' SQLite ha culminado.")

    except ValueError as val_err:
        logger.error(f"Configuración de entorno perdida: {env_var_to_test}")
        console.print(Panel(
            f"❌ [bold red]Variable no definida en el .env[/bold red]\n"
            f"[white]💡 Solución: Agrega al .env: [bold yellow]{env_var_to_test}='data/output/db_local.db'[/bold yellow][/white]",
            border_style="red"
        ))
    except Exception as e:
        logger.exception(f"Fallo interactuando con SQLite en '{env_var_to_test}'")
        console.print(Panel(
            f"❌ [bold red]Fallo Operativo SQLite[/bold red]\n"
            f"[dim]Mensaje error: {e}[/dim]",
            border_style="red"
        ))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prrueba tu SQLite local.")
    parser.add_argument("--env", dest="env_var", default="DB_SQLITE", help="Variable de SQLite (Default: DB_SQLITE).")
    args = parser.parse_args()
    
    run_test(args.env_var)
