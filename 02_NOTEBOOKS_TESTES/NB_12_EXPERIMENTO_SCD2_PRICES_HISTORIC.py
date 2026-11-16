# Databricks notebook source
# Configurar os caminhos
import os
import sys

BASE = "/Workspace/Users/brewwerynyc@gmail.com/PROJETO_VISAO_UNICA_PRECOS"

ROOT = "/Volumes/workspace/default/dados_teste_visao_unica"

SCRIPT = (
    f"{BASE}/01_CODIGO_AS_IS/visaounicapreco/"
    "visao_unica_preco/etls/trusted/"
    "experimento_scd2_prices_historic.py"
)

ORIGEM = f"{ROOT}/resultados/prices_historic"
DESTINO = f"{ROOT}/resultados/prices_historic_scd2"

print("Script encontrado:", os.path.isfile(SCRIPT))
print("Dados de origem encontrados:", os.path.isdir(ORIGEM))
print("Diretório de destino já existe:", os.path.isdir(DESTINO))

# COMMAND ----------

# Carregar Sness
import types
import importlib.util

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

from utils import config_spark, read_visao_unica_dataset

print("Utils carregado com sucesso")
print("Ambientes:", [(e.name, e.value) for e in Environment])

# COMMAND ----------

# Verificar o histórico de origem
arquivos = sorted(
    os.path.join(ORIGEM, nome)
    for nome in os.listdir(ORIGEM)
    if nome.endswith(".parquet")
)

print("Arquivos Parquet:", len(arquivos))

for arquivo in arquivos:
    df_arquivo = spark.read.parquet(arquivo)

    print(
        os.path.basename(arquivo),
        "| colunas:", len(df_arquivo.columns)
    )

# COMMAND ----------

# Preparar leitura compatível do histórico
from functools import reduce
from pyspark.sql import functions as sf

SCHEMA_REFERENCIA = spark.read.parquet(
    f"{ROOT}/parquet/prices_historic"
).schema

dataframes = []

for arquivo in arquivos:
    df = spark.read.parquet(arquivo)

    df = df.select(*[
        (
            sf.col(c.name).cast(c.dataType)
            if c.name in df.columns
            else sf.lit(None).cast(c.dataType)
        ).alias(c.name)
        for c in SCHEMA_REFERENCIA.fields
    ])

    dataframes.append(df)

if not dataframes:
    raise ValueError("Nenhum arquivo Parquet encontrado")

df_historico = reduce(
    lambda a, b: a.unionByName(b),
    dataframes
)

print("Registros do histórico:", df_historico.count())
print("Colunas:", len(df_historico.columns))

# COMMAND ----------

# Executar o SCD2 original
import runpy
import sys

sys.argv = [
    SCRIPT,
    "--environment", "stg",
    "--start_date", "2026-01-01",
    "--end_date", "2026-12-31",
    "--dataset_destino", "prices_historic_scd2"
]

codigo = compile(
    open(SCRIPT, encoding="utf-8").read(),
    SCRIPT,
    "exec"
)

namespace = {
    "__name__": "teste_scd2",
    "__file__": SCRIPT
}

exec(codigo, namespace)

def leitura_poc(ss, environment, start_date, end_date):
    df = df_historico

    if start_date:
        df = df.filter(
            sf.col("price_date") >= sf.lit(start_date).cast("date")
        )

    if end_date:
        df = df.filter(
            sf.col("price_date") <= sf.lit(end_date).cast("date")
        )

    return df

namespace["ler"] = leitura_poc

print("Iniciando ETL SCD2...")

resultado = namespace["main"]()

print("Código de retorno:", resultado)
print("ETL SCD2 executado com sucesso!")

# COMMAND ----------

# Validar arquivos gerados
print("Diretório existe:", os.path.isdir(DESTINO))

if os.path.isdir(DESTINO):
    print("Arquivos gerados:")

    for arquivo in os.listdir(DESTINO)[:20]:
        print(" -", arquivo)
else:
    raise FileNotFoundError(
        f"Diretório de saída não encontrado: {DESTINO}"
    )

# COMMAND ----------

# Validar resultado SCD2
df_scd2 = spark.read.parquet(DESTINO)

print("Registros SCD2:", df_scd2.count())
print("Colunas:", len(df_scd2.columns))

df_scd2.printSchema()

display(
    df_scd2.select(
        "seller_id",
        "navigation_id",
        "sku",
        "campaign_code",
        "valid_from",
        "valid_to",
        "is_current",
        "snapshots_colapsados"
    ).orderBy(
        "seller_id",
        "sku",
        "valid_from"
    )
)

# COMMAND ----------

# Verificação adicional — Integridade dos períodos
from pyspark.sql import functions as sf

print(
    "Períodos atuais:",
    df_scd2.filter(sf.col("is_current") == True).count()
)

print(
    "Períodos encerrados:",
    df_scd2.filter(sf.col("is_current") == False).count()
)

print(
    "Registros com valid_from nulo:",
    df_scd2.filter(sf.col("valid_from").isNull()).count()
)

print(
    "Períodos com datas inválidas:",
    df_scd2.filter(
        sf.col("valid_to").isNotNull()
        & (sf.col("valid_to") <= sf.col("valid_from"))
    ).count()
)