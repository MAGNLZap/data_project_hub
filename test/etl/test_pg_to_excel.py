import os
import sys
import argparse
from dotenv import load_dotenv

# 1. Primero configuramos la ruta para que Python encuentre la raíz del proyecto
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# 2. Ahora sí podemos importar nuestros módulos locales
from connections.postgres import PostgresConnector
from utils.writers.excel_writer import write_to_excel
from utils.logs.logging import logger

from rich.console import Console
from rich.panel import Panel

load_dotenv()

def run_extraction_test(env_var: str, dbname: str, schema: str, table: str, output_file: str):
    console = Console()

    console.print(Panel.fit(
        f"🚀 [bold cyan]Orquestador Dinámico ETL: PostgreSQL -> Excel[/bold cyan]\n\n"
        f"🎯 [dim]Origen:[/dim] Servidor: [bold yellow]{env_var}[/bold yellow] | Instancia: [bold yellow]{dbname}[/bold yellow]\n"
        f"🔍 [dim]Objetivo:[/dim] {schema}.{table}\n"
        f"📦 [dim]Destino:[/dim] /data/output/{output_file}",
        border_style="cyan"
    ))

    # Lógica de Límite: 0 o error significa PROCESAR TODO
    limit_val = os.getenv("LIMIT", "0").strip()
    
    try:
        limite_entero = int(limit_val)
    except:
        limite_entero = 0 # Error de formato = Procesar todo por seguridad
        
    # Construcción de la Query final
    query = f"SELECT * FROM {schema}.{table}"
    if limite_entero > 0:
        query += f" LIMIT {limite_entero}"
        logger.info(f"Aplicando límite parcial de {limite_entero} registros para la muestra.")
    else:
        logger.info(f"Extrayendo set de datos completo (Sin límites).")

    try:
        with console.status(f"[bold yellow]Consultando origen de datos... Fetching...", spinner="bouncingBar"):
            # 1. Extracción
            with PostgresConnector(env_var, dbname=dbname) as db_conn:
                dataset = db_conn.execute_query(query)

            volumen = len(dataset)
            if volumen == 0:
                logger.warning(f"La consulta no extrajo datos de {schema}.{table}")
                console.print(Panel(
                    "⚠️ [yellow]Extracción Vacía[/yellow]. La tabla existe pero no tiene filas que analizar.", border_style="yellow"))
                return

        console.print(f"📥 [bold green]Extracción Completa :[/bold green] [bold]{volumen}[/bold] registros bajados a memoria.")

        with console.status(f"[bold yellow]Cocinando Excel a través del Writer y Ajustando columnas...", spinner="aesthetic"):
            # 2. Volcado a Excel
            export_path = write_to_excel(data=dataset, filename=output_file, sheet_name=table[:30])

        console.print(Panel(
            f"✅ [bold green]Misión Lograda Exitosamente[/bold green]\n"
            f"El motor construyó la hoja auto-estética preservando tipados de db en tu archivo local:\n"
            f"[dim]{export_path}[/dim]",
            border_style="green"
        ))

    except Exception as e:
        logger.exception(f"Fallo cataclísmico cruzando PostgreSQL {schema}.{table} -> Excel")
        console.print(Panel(
            f"❌ [bold red]El flujo fue Interrumpido e Inconcluso[/bold red]\n"
            f"Verifique si la tabla existe o si tiene los permisos de lectura de la bd enviada.\n"
            f"[dim]Código del error: {str(e)}[/dim]",
            border_style="red"
        ))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prueba Cruzada End-To-End (PG -> Excel)")

    # Orígenes parametrizados de BD
    parser.add_argument("--env", dest="env_var", default="DBPG_DWH", help="Variable Raíz al PG (Dft: DBPG_DWH)")
    parser.add_argument("--db", dest="dbname", default="postgres", help="Nombre BaseDatos a escarbar")

    # Orígenes parametrizados de Tablas/Esquemas
    parser.add_argument("--schema", dest="schema", default="public", help="Esquema SQL (Default: public)")
    parser.add_argument("--table", dest="table", required=True, help="OBLIGATORIO: Nombre bruto de la tabla a extraer")

    # Desembocadura
    parser.add_argument("--out", dest="output", default="extrac_dump.xlsx", help="Re-nombra tu archivo final (.xlsx)")

    args = parser.parse_args()

    run_extraction_test(args.env_var, args.dbname, args.schema, args.table, args.output)
