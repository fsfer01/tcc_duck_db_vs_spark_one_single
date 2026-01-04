import time
import pandas as pd
import os
import glob
from pandasql import sqldf
from datetime import datetime, timedelta
from src.utils import SystemMonitor, registrar_execucao

# ---------------------------------------------------
# VARIÁVEIS GLOBAIS
# ---------------------------------------------------
data_atual = (datetime.now() - timedelta(hours=3)).strftime("%Y-%m-%d %Hh:%Mmin")
tamanho_do_dado = "100GB"
engine = "pandasql"
# no topo do seu script
start_total = time.time()

# ---------------------------------------------------
# Caminhos
# ---------------------------------------------------
parquet_path = "/media/sf_HD_EXTERNO/bucket/100_gb/parquets/*.parquet"
parquet_files = glob.glob(parquet_path)

sql_folder = "/media/sf_HD_EXTERNO/bucket/100_gb/consultas_sql/*.sql"
sql_files = sorted(glob.glob(sql_folder))

if not parquet_files:
    raise RuntimeError("Nenhum parquet encontrado")

if not sql_files:
    raise RuntimeError("Nenhum SQL encontrado")

timestamp_str = (datetime.now() - timedelta(hours=3)).strftime("%Y_%m_%d")

# ---------------------------------------------------
# Loop nas queries
# ---------------------------------------------------
for sql_file in sql_files:
    query_name = os.path.basename(sql_file).replace(".sql", "")
    codigo = f"pandasql_incrementental_{query_name}_{tamanho_do_dado}"

    log_filename = f"logs/pandasql_{query_name}_{timestamp_str}_{tamanho_do_dado}.csv"

    monitor = SystemMonitor(
        output_file=log_filename,
        script_name=codigo,
        engine="python"
    )
    monitor.start_monitoring()

    print(f"\n▶ Executando query: {query_name}")

    start_query = time.time()  # início da query
    status = "success"

    try:
        # Loop nos parquets
        for file_path in parquet_files:
            file_name = os.path.basename(file_path)
            #print(f"  → Parquet: {file_name}")

            df = pd.read_parquet(file_path)
            locals_dict = {"tabela": df}

            with open(sql_file, "r") as f:
                query_sql = f.read()

            result_df = sqldf(query_sql, locals_dict)
            result = result_df.values.tolist()
            #print(f"    Resultado: {result}")

    except Exception as e:
        print(f"❌ Erro na query {query_name}: {e}")
        status = "error"

    finally:
        registrar_execucao(
            data_atual=data_atual,
            codigo=codigo,
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

# no final, antes do print final
end_total = time.time()
tempo_total = end_total - start_total
minutos = int(tempo_total // 60)
segundos = int(tempo_total % 60)

print(f"\n⏱ Tempo total de execução do script: {minutos} min {segundos} s")

print("pandas_incremental_100gb - Todas as consultas finalizadas.")
