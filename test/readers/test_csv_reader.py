import os
import sys
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.readers.csv_reader import read_csv_normalized
from utils.logs.logging import logger

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

def run_test(filename):
    console = Console()
    
    console.print(Panel.fit(
        f"📝 [bold magenta]Módulo Visualizador de CSVs Planos[/bold magenta]\n"
        f"Persiguiendo objetivo plano: [bold yellow]{filename}[/bold yellow]",
        border_style="magenta"
    ))

    try:
        with console.status("[bold yellow]Olfateando matriz y decodificando valores UTF-8...", spinner="dots2"):
            # Probamos en modo de piloto automático absoluto (sin entregar delimitador)
            df_limpio = read_csv_normalized(filename)
            
        console.print(f"\n[bold green]✅ Ingesta CSV Exitosa![/bold green] Se extrajeron [bold]{len(df_limpio)}[/bold] registros vivos y [bold]{len(df_limpio.columns)}[/bold] columnas depuradas.")
        
        table = Table(show_header=True, header_style="bold cyan", border_style="cyan")
        table.add_column("Columna Estandarizada", style="bold green", width=25)
        table.add_column("Tipo de Dato Interno", style="dim")
        table.add_column("Anomalías Vacías", justify="right")

        for col in df_limpio.columns:
            nulos = df_limpio[col].isna().sum()
            tipo = str(df_limpio[col].dtype)
            table.add_row(col, tipo, str(nulos))

        console.print(table)
        
        console.print("\n[dim]Vista previa de extracción (Tres datos aleatorios crudos)[/dim]:")
        console.print(df_limpio.head(3).to_markdown())
        
    except FileNotFoundError as err:
        console.print(Panel(
            f"❌ [bold red]Vector extraviado[/bold red]\n"
            f"[dim]{err}[/dim]\n"
            f"[white]Sube este archivo simulado a root[yellow]/data/input/[/yellow][/white]",
            border_style="red"
        ))
    except Exception as e:
        logger.exception("Catástrofe detonada dentro de test_csv_reader")
        console.print(Panel(f"❌ Fallo masivo al cruzar datos CSV: {e}", border_style="red"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prueba del estandarizador de Planos.")
    parser.add_argument("--file", dest="filename", default="datos.csv", help="Archivo objetivo (incluyendo subfijo .csv).")
    args = parser.parse_args()
    
    run_test(args.filename)
