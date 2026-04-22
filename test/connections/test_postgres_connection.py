import os
import sys
import argparse

# Tratamos de cargar el entorno del proyecto
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connections.postgres import PostgresConnector
from utils.logs.logging import logger

# Importamos las herramientas gráficas de consola
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import print as rprint

def run_test(env_var_to_test, dbname):
    """
    Script de prueba visual interactiva.
    """
    console = Console()
    
    # Un panel azul amigable de bienvenida
    console.print(Panel.fit(
        f"🤖 [bold cyan]Prueba de Conexión de Base de Datos[/bold cyan]\n"
        f"Servidor: [bold yellow]{env_var_to_test}[/bold yellow] | Base de Datos: [bold yellow]{dbname}[/bold yellow]",
        border_style="cyan"
    ))

    # Trazamos el intento en nuestros logs
    logger.info(f"Usuario ejecutando prueba hacia '{env_var_to_test}', Base de datos: '{dbname}'")

    try:
        # Aquí inicia la animación visual (Spinner) de progreso
        with console.status(f"[bold yellow]Conectando a la DB '{dbname}' en el servidor '{env_var_to_test}'...", spinner="dots") as status:
            
            with PostgresConnector(env_var_to_test, dbname=dbname) as db_conn:
                # Simulamos ejecutar un query en postgres
                query = "SELECT 1 as control_test, current_timestamp as server_time"
                result = db_conn.execute_query(query)

        # Si todo salió bien, preparamos una Tabla elegante para mostrar los datos
        table = Table(show_header=True, header_style="bold magenta", border_style="magenta")
        table.add_column("Columna", style="dim", width=20)
        table.add_column("Valor extraído", style="bold green")

        for row in result:
            table.add_row("control_test", str(row.get('control_test')))
            table.add_row("server_time", str(row.get('server_time')))

        console.print(table)
        console.print(Panel(f"✅ [bold green]Tu conexión a '{dbname}' funciona perfectamente.[/bold green]\nEl test ha finalizado y se ha desconectado.", border_style="green"))
        
        # Validamos en logs internos
        logger.success(f"La prueba hacia '{env_var_to_test}/{dbname}' fue un éxito.")

    except ValueError as val_err:
        logger.error(f"Fallo de configuración: variable '{env_var_to_test}' vacía o no encontrada en .env")
        console.print(Panel(
            f"❌ [bold red]Falta configuración en tu .env[/bold red]\n"
            f"Mensaje del sistema: {val_err}\n"
            f"[white]💡 Solución: Asegúrate de que exista [bold yellow]{env_var_to_test}='postgresql://tuURL:5432'[/bold yellow][/white]",
            border_style="red"
        ))
    except Exception as e:
        logger.exception(f"Fallo conectando a '{dbname}' a través del Sandbox de Usuario.")
        console.print(Panel(
            f"❌ [bold red]Fallo de comunicación con la Base de datos[/bold red]\n"
            f"El ordenador intentó conectar pero fue rechazado. Esto suele pasar si:\n"
            f"- La base de datos '{dbname}' no existe en ese servidor.\n"
            f"- Las credenciales o el host son incorrectos.\n\n"
            f"[dim]Mensaje error: {e}[/dim]",
            border_style="red",
            title="Detalle del Error"
        ))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prueba la conexión a una BD.")
    parser.add_argument("--env", dest="env_var", default="DBPG_DWH", help="Variable del servidor .env (Raíz).")
    parser.add_argument("--db", dest="dbname", default="postgres", help="Nombre de la Base de datos a atacar (Default: postgres).")
    args = parser.parse_args()
    
    run_test(args.env_var, args.dbname)

