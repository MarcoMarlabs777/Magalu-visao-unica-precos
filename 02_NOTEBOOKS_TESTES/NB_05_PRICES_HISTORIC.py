# Databricks notebook source
# Carregar as bibliotecas Sness
import os
import sys
import types
import importlib.util

BASE = "/Workspace/Users/brewwerynyc@gmail.com/PROJETO_VISAO_UNICA_PRECOS"

COMMON = f"{BASE}/03_CONFIGURAÇÕES/BIBLIOTECA_SNESS/sness-common-main/sness"
SPARK = f"{BASE}/03_CONFIGURAÇÕES/BIBLIOTECA_SNESS/sness-spark-main/sness"

os.environ["SNESS_DATABRICKS_SERVERLESS"] = "true"

sness = types.ModuleType("sness")
sness.__path__ = [COMMON, SPARK]
sys.modules["sness"] = sness

datalake = types.ModuleType("sness.datalake")
datalake.__path__ = [
    f"{COMMON}/datalake",
    f"{SPARK}/datalake"
]
sys.modules["sness.datalake"] = datalake
sness.datalake = datalake

spec = importlib.util.spec_from_file_location(
    "sness.settings",
    f"{SPARK}/settings/__init__.py",
    submodule_search_locations=[
        f"{COMMON}/settings",
        f"{SPARK}/settings"
    ]
)

settings = importlib.util.module_from_spec(spec)
sys.modules["sness.settings"] = settings
sness.settings = settings
spec.loader.exec_module(settings)

from sness.datalake.metadata import Zone, Environment
from sness.datalake.sness_spark import SnessSpark

print("Sness carregado com sucesso")
print("Spark:", spark.version)

# COMMAND ----------

# Configurar os imports do projeto
import sys

UTILS_DIR = (
    f"{BASE}/01_CODIGO_AS_IS/visaounicapreco/"
    "visao_unica_commons/etls/utils"
)

ETL_DIR = (
    f"{BASE}/01_CODIGO_AS_IS/visaounicapreco/"
    "visao_unica_preco/etls/trusted"
)

for caminho in [UTILS_DIR, ETL_DIR]:
    if caminho not in sys.path:
        sys.path.insert(0, caminho)

from utils import config_spark, parse_args, read_visao_unica_dataset

SCRIPT = f"{ETL_DIR}/prices_historic_last_90_days.py"

print("Utils carregado com sucesso")
print("Script encontrado:", os.path.isfile(SCRIPT))

# COMMAND ----------

# Executar o ETL
import sys
import runpy

sys.argv = [
    SCRIPT,
    "--environment", "stg",
    "--days_to_process", "90"
]

print("Iniciando ETL...")

runpy.run_path(SCRIPT, run_name="__main__")

print("ETL executado com sucesso!")

# COMMAND ----------

# Validar os arquivos gerados
import os

SAIDA = (
    "/Volumes/workspace/default/dados_teste_visao_unica/"
    "resultados/prices_historic_last_90_days"
)

print("Diretório existe:", os.path.isdir(SAIDA))

if os.path.isdir(SAIDA):
    print("Arquivos:", os.listdir(SAIDA)[:20])
else:
    print("ERRO: O ETL não criou o diretório de saída.")

# COMMAND ----------

# Validar os dados
df = spark.read.parquet(SAIDA)

print("Quantidade de registros:", df.count())
print("Quantidade de colunas:", len(df.columns))

df.printSchema()

display(df.limit(10))

# COMMAND ----------

