# Databricks notebook source
# Configurar os caminhos
import os
import sys

BASE = "/Workspace/Users/brewwerynyc@gmail.com/PROJETO_VISAO_UNICA_PRECOS"

ROOT = "/Volumes/workspace/default/dados_teste_visao_unica"

SCRIPT = (
    f"{BASE}/01_CODIGO_AS_IS/visaounicapreco/"
    "visao_unica_preco/etls/trusted/fetch_catalogo.py"
)

PRICES = f"{ROOT}/resultados/prices"

print("Script encontrado:", os.path.isfile(SCRIPT))
print("Dados prices encontrados:", os.path.isdir(PRICES))

# COMMAND ----------

# Carregar a biblioteca Sness
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

from sness.datalake.sness_spark import SnessSpark

print("Sness carregado com sucesso")
print("Spark:", spark.version)

# COMMAND ----------

# Validar dependências
import importlib.util

dependencias = [
    "requests",
    "multiprocessing",
    "decimal",
    "dataclasses",
    "pyspark"
]

for biblioteca in dependencias:
    encontrada = importlib.util.find_spec(biblioteca) is not None

    print(
        biblioteca,
        "OK" if encontrada else "NÃO ENCONTRADA"
    )

# COMMAND ----------

# Carregar as funções originais
import runpy

if not os.path.isfile(SCRIPT):
    raise FileNotFoundError(SCRIPT)

modulo = runpy.run_path(
    SCRIPT,
    run_name="teste_fetch_catalogo"
)

ProdutoFromCatalogo = modulo["ProdutoFromCatalogo"]
get_data_from_navigation_id = modulo["get_data_from_navigation_id"]
parse_prices_from_data = modulo["parse_prices_from_data"]
fetch_prices = modulo["fetch_prices"]

print("Funções carregadas com sucesso")

for nome in [
    "get_data_from_navigation_id",
    "parse_prices_from_data",
    "fetch_prices"
]:
    print(" -", nome)

# COMMAND ----------

# Validar a leitura dos preços
from pyspark.sql import functions as sf
from pyspark.sql.types import StringType

df_prices = spark.read.parquet(PRICES)

print("Registros:", df_prices.count())
print("Colunas originais:", len(df_prices.columns))

colunas_necessarias = [
    "navigation_id",
    "sku",
    "seller_id",
    "price_de",
    "price_por",
    "price_pix",
    "discount_perc_pix",
    "active",
    "stock"
]

for coluna in colunas_necessarias:
    if coluna not in df_prices.columns:
        print("Coluna ausente:", coluna)
        df_prices = df_prices.withColumn(
            coluna,
            sf.lit(None).cast(StringType())
        )

print("Colunas após compatibilização:", len(df_prices.columns))

display(
    df_prices.select(*colunas_necessarias).limit(10)
)

# COMMAND ----------

# Selecionar os produtos para validação
df_amostra = (
    df_prices
    .filter(sf.col("active") & sf.col("stock"))
    .orderBy(sf.rand())
    .limit(10)
    .select(
        "navigation_id",
        "sku",
        "seller_id",
        "price_de",
        "price_por",
        "price_pix",
        "discount_perc_pix"
    )
)

df_amostra.cache()

nav_ids = [
    row["navigation_id"]
    for row in df_amostra.collect()
]

print("Produtos selecionados:", len(nav_ids))

display(df_amostra)

# COMMAND ----------

# Validar o parser original sem API
from decimal import Decimal

navigation_id_teste = (
    nav_ids[0] if nav_ids else "PRODUTO_TESTE_001"
)

resposta_simulada = {
    "data": {
        "price": Decimal("100.00"),
        "variations": [
            {
                "id": navigation_id_teste,
                "sellers": [
                    {
                        "price": Decimal("90.00"),
                        "best_price": {
                            "payment_method_id": "pix",
                            "total_amount": Decimal("85.00"),
                            "discount": Decimal("5.00")
                        }
                    }
                ]
            }
        ]
    }
}

produto = parse_prices_from_data(
    resposta_simulada,
    navigation_id_teste
)

print("Produto:", produto)

assert produto is not None
assert produto.price_de == Decimal("100.00")
assert produto.price_por == Decimal("90.00")
assert produto.price_pix == Decimal("85.00")

print("Parser validado com sucesso")

# COMMAND ----------

# Validar criação do DataFrame Spark
from pyspark.sql import Row

dados_api = [produto.toRow()]

df_api = spark.createDataFrame(dados_api)

for coluna in df_api.columns:
    df_api = df_api.withColumnRenamed(
        coluna,
        f"api_{coluna}"
    )

print("DataFrame da API criado com sucesso")

df_api.printSchema()

display(df_api)

# COMMAND ----------

# Validar a comparação dos preços
df_comparacao = df_amostra.join(
    df_api,
    how="inner",
    on=(
        sf.col("navigation_id")
        == sf.col("api_navigation_id")
    )
)

df_divergencias = (
    df_comparacao
    .withColumn(
        "right",
        sf.col("price_pix") == sf.col("api_price_pix")
    )
    .filter(
        (sf.col("right") == False)
        & (
            sf.abs(
                sf.col("api_price_pix") - sf.col("price_pix")
            ) >= sf.lit(0.1)
        )
    )
)

print("Registros comparados:", df_comparacao.count())
print("Divergências encontradas:", df_divergencias.count())

display(df_divergencias)

# COMMAND ----------

# Verificar conectividade com a API interna (opcional)
import socket

HOST_API = "catalogo-internal.luizalabs.com"

try:
    ip = socket.gethostbyname(HOST_API)

    print("DNS resolvido com sucesso")
    print("Host:", HOST_API)
    print("IP:", ip)

except Exception as e:
    print("Não foi possível resolver o endereço da API")
    print("Erro:", type(e).__name__, str(e))

# COMMAND ----------

# Conectividade HTTP
import socket

HOST = "catalogo-internal.luizalabs.com"
PORTA = 80

try:
    with socket.create_connection(
        (HOST, PORTA),
        timeout=5
    ):
        print("Conexão TCP estabelecida com sucesso")

except socket.timeout:
    print("TIMEOUT: API não respondeu à conexão")

except OSError as e:
    print("Falha na conexão:", type(e).__name__, str(e))