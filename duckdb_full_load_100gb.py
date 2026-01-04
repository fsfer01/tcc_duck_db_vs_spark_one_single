import time
import duckdb
import os
import glob
from datetime import datetime, timedelta
from src.utils import SystemMonitor, registrar_execucao

# ---------------------------------------------------
# VARIÁVEIS GLOBAIS
# ---------------------------------------------------

BASE_DIR = os.getcwd()

DUCKDB_TMP_DIR = os.path.join(BASE_DIR, "duckdb_tmp")

os.makedirs(DUCKDB_TMP_DIR, exist_ok=True)

data_atual = (datetime.now() - timedelta(hours=3)).strftime("%Y-%m-%d %Hh:%Mmin")
codigo = 'duckdb_full_load'
tamanho_do_dado = '100GB'


# tempo total do script
start_total = time.time()

# ---------------------------------------------------
# Caminhos
# ---------------------------------------------------
parquet_path = "/media/sf_HD_EXTERNO/bucket/100_gb/parquets/*.parquet"
sql_folder = "/media/sf_HD_EXTERNO/bucket/100_gb/consultas_sql/*.sql"

# apenas para validação (opcional)
parquet_files = glob.glob(parquet_path)
if not parquet_files:
    raise RuntimeError("Nenhum parquet encontrado")

sql_files = sorted(glob.glob(sql_folder))
if not sql_files:
    raise RuntimeError("Nenhum SQL encontrado")

timestamp_str = (datetime.now() - timedelta(hours=3)).strftime("%Y_%m_%d")

# ---------------------------------------------------
# DuckDB
# ---------------------------------------------------
con = duckdb.connect()

# garante alinhamento com limite do container (4 CPUs)
con.execute("PRAGMA threads=4")

# Diretório temporário para spill em disco
con.execute(f"PRAGMA temp_directory='{DUCKDB_TMP_DIR}'")

# cria a VIEW UMA ÚNICA VEZ lendo todos os parquets
con.execute(f"""
    CREATE OR REPLACE VIEW tabela AS
    SELECT * FROM read_parquet('{parquet_path}')
""")

# ---------------------------------------------------
# Loop nas queries
# ---------------------------------------------------
for sql_file in sql_files:
    query_name = os.path.basename(sql_file).replace(".sql", "")
    codigo_query = f"{codigo}_{query_name}_{tamanho_do_dado}"

    log_filename = f"logs/{codigo}_{query_name}_{timestamp_str}_{tamanho_do_dado}.csv"

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
        result = con.execute(query_sql).fetchall()

    except Exception as e:
        print(f"    ❌ Erro: {e}")
        status = "error"

    finally:
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

# ---------------------------------------------------
# Finalização
# ---------------------------------------------------
time.sleep(5)

end_total = time.time()
tempo_total = end_total - start_total

minutos = int(tempo_total // 60)
segundos = int(tempo_total % 60)

print(f"\n⏱ Tempo total de execução do script: {minutos} min {segundos} s")
print("duckdb_full_load_100gb - Todas as consultas finalizadas.")
