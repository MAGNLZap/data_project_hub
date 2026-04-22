import os
import sys
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.readers.excel_reader import read_excel_normalized
from utils.logs.logging import logger

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

def run_test(filename):
    console = Console()
    
    console.print(Panel.fit(
        f"🧹 [bold cyan]Prueba del Normalizador Universal de Excel[/bold cyan]\n"
        f"Archivo objetivo a buscar: [bold yellow]{filename}[/bold yellow]",
        border_style="cyan"
    ))

    try:
        with console.status("[bold yellow]Buscando, leyendo y limpiando archivo...", spinner="aesthetic"):
            # Probamos nuestra función que limpia, estandariza y parsea
            df_limpio = read_excel_normalized(filename)
            
        console.print(f"\n[bold green]✅ ¡Procesamiento Exitoso![/bold green] Se detectaron [bold]{len(df_limpio)}[/bold] registros validables y [bold]{len(df_limpio.columns)}[/bold] columnas depuradas.")
        
        # Mostramos estadísticas de columnas en Rich Table
        table = Table(show_header=True, header_style="bold magenta", border_style="magenta")
        table.add_column("Columna", style="bold green", width=25)
        table.add_column("Tipo de Dato Automático", style="dim")
        table.add_column("Valores Nulos", justify="right")

        for col in df_limpio.columns:
            nulos = df_limpio[col].isna().sum()
            tipo = str(df_limpio[col].dtype)
            table.add_row(col, tipo, str(nulos))

        console.print(table)
        
        # Pintamos un snapshot visual de 3 filas para corroborarlo
        console.print("\n[dim]Vista previa cruda del DataFrame limpio (Primeros 3 registros)[/dim]:")
        console.print(df_limpio.head(3).to_markdown())
        
    except FileNotFoundError as f_err:
        console.print(Panel(
            f"❌ [bold red]El Archivo No Existe[/bold red]\n"
            f"[dim]{f_err}[/dim]\n"
            f"[white]Asegúrate de haber guardado físicamente tu archivo en [yellow]/data/input/[/yellow][/white]",
            border_style="red"
        ))
    except Exception as e:
        logger.exception("Detonó una excepción en el test_excel_reader")
        console.print(Panel(f"❌ Fallo inesperado: {e}", border_style="red"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prueba del estandarizador de Excels.")
    parser.add_argument("--file", dest="filename", default="reporte.xlsx", help="Nombre del excel con su extensión.")
    args = parser.parse_args()
    
    run_test(args.filename)
