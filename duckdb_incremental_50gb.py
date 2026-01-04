import time
import duckdb
import os
import glob
from datetime import datetime, timedelta
from src.utils import SystemMonitor, registrar_execucao

# ---------------------------------------------------
# VARIÁVEIS GLOBAIS
# ---------------------------------------------------
data_atual = (datetime.now() - timedelta(hours=3)).strftime("%Y-%m-%d %Hh:%Mmin")
codigo = 'duckdb_incremental'
tamanho_do_dado = '50GB'
# no topo do seu script
start_total = time.time()


# ---------------------------------------------------
# Caminhos
# ---------------------------------------------------
parquet_path = "/media/sf_HD_EXTERNO/bucket/50_gb/parquets/*.parquet"
parquet_files = glob.glob(parquet_path)

sql_folder = "/media/sf_HD_EXTERNO/bucket/50_gb/consultas_sql/*.sql"
sql_files = sorted(glob.glob(sql_folder))

if not parquet_files:
    raise RuntimeError("Nenhum parquet encontrado")

if not sql_files:
    raise RuntimeError("Nenhum SQL encontrado")

timestamp_str = (datetime.now() - timedelta(hours=3)).strftime("%Y_%m_%d")

# ---------------------------------------------------
# DuckDB
# ---------------------------------------------------
con = duckdb.connect()

# ---------------------------------------------------
# Loop nas queries
# ---------------------------------------------------
for sql_file in sql_files:
    query_name = os.path.basename(sql_file).replace(".sql", "")
    codigo_query = f"{codigo}_{query_name}_{tamanho_do_dado}"

    log_filename = f"logs/duckdb_{query_name}_{timestamp_str}_{tamanho_do_dado}.csv"

    monitor = SystemMonitor(
        output_file=log_filename,
        script_name=codigo_query,
        engine="python"
    )
    monitor.start_monitoring()

    print(f"\n▶ Executando query: {query_name}")

    # Ler SQL uma vez
    with open(sql_file, "r") as f:
        query_sql = f.read()

    start_query = time.time()
    status = "success"

    try:
        # Loop nos parquets
        for file_path in parquet_files:
            file_name = os.path.basename(file_path)
            #print(f"  → Parquet: {file_name}")

            # Cria view temporária igual ao Spark
            con.execute(f"""
                CREATE OR REPLACE VIEW tabela AS
                SELECT * FROM read_parquet('{file_path}')
            """)

            result = con.execute(query_sql).fetchall()
            #print(f"    Resultado: {result}")

    except Exception as e:
        print(f"    ❌ Erro: {e}")
        status = "error"

    finally:
        # Log do tempo total da query
        registrar_execucao(
            data_atual=data_atual,
            codigo=codigo_query,
            tamanho_do_dado=tamanho_do_dado,
            start=start_query,
            end=time.time(),
            status=status,
            type="sql",
            description=query_name
        )
        monitor.stop_monitoring()

time.sleep(5)

# no final, antes do print final
end_total = time.time()
tempo_total = end_total - start_total
minutos = int(tempo_total // 60)
segundos = int(tempo_total % 60)

print(f"\n⏱ Tempo total de execução do script: {minutos} min {segundos} s")

print("duckdb_incremental_50gb - Todas as consultas finalizadas.")

