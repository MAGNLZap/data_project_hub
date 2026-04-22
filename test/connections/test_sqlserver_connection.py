import os
import sys
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connections.sqlserver import SqlServerConnector
from utils.logs.logging import logger

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

def run_test(env_var_to_test, dbname):
    """
    Script de prueba visual interactiva.
    """
    console = Console()
    
    console.print(Panel.fit(
        f"🤖 [bold magenta]Prueba de Conexión Microsoft SQL Server[/bold magenta]\n"
        f"Servidor: [bold yellow]{env_var_to_test}[/bold yellow] | Base de Datos: [bold yellow]{dbname}[/bold yellow]",
        border_style="magenta"
    ))

    logger.info(f"Usuario ejecutando prueba hacia MS SQL '{env_var_to_test}', Base de datos: '{dbname}'")

    try:
        # Aquí inicia la animación visual usando rich
        with console.status(f"[bold yellow]Negociando enlace con SQL Server en la base '{dbname}'...", spinner="dots") as status:
            
            with SqlServerConnector(env_var_to_test, dbname=dbname) as db_conn:
                # Comprobación de identidad en MSSQL (Dará la versión del motor y la BD)
                query = "SELECT @@VERSION as server_version, DB_NAME() as target_db"
                result = db_conn.execute_query(query)

        # Si supera el assert extraemos la tabla de demostración
        table = Table(show_header=True, header_style="bold blue", border_style="blue")
        table.add_column("Propiedad Evaluada", style="dim", width=20)
        table.add_column("Métrica del Servidor", style="bold green")

        for row in result:
            table.add_row("Base Local Atacada", str(row.get('target_db')))
            table.add_row("Motor Instanciado", str(row.get('server_version'))[:55] + "...")

        console.print(table)
        console.print(Panel(f"✅ [bold green]Enlace a '{dbname}' certificado con éxito por PyODBC.[/bold green]", border_style="green"))
        
        logger.success(f"Escudo superado. La prueba de conectividad hacia MS SQL '{env_var_to_test}/{dbname}' fue un éxito pleno.")

    except ValueError as val_err:
        logger.error(f"Eslabón Roto: Variable '{env_var_to_test}' ilocalizable en config local")
        console.print(Panel(
            f"❌ [bold red]Falta configuración de MS SQL en el .env[/bold red]\n"
            f"[white]💡 Solución: Asegúrate de tener al menos una cadena del estilo:\n"
            f"[bold yellow]DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=ip;UID=x;PWD=y[/bold yellow][/white]",
            border_style="red"
        ))
    except Exception as e:
        logger.exception(f"Error bloqueante de SQL Server asaltando '{dbname}'")
        console.print(Panel(
            f"❌ [bold red]Fallo grave de driver Windows o Credenciales[/bold red]\n"
            f"- Asegúrate de tener el driver instalado en tu windows.\n"
            f"- Verifica el usuario/password o si es una red VPN restringida.\n\n"
            f"[dim]Mensaje error: {e}[/dim]",
            border_style="red",
            title="Detalle del Error"
        ))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prueba la conexión a un MS SQL Server.")
    parser.add_argument("--env", dest="env_var", default="DB_MSSQL", help="Variable Raíz SQL Server.")
    parser.add_argument("--db", dest="dbname", default="master", help="Nombre de la Base de datos (Default: master).")
    args = parser.parse_args()
    
    run_test(args.env_var, args.dbname)
