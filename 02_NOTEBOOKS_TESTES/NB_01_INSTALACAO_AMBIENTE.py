# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Identificando instalação SNESS
import importlib.util

for modulo in [
    "sness",
    "sness.datalake",
    "sness.datalake.metadata",
    "sness.datalake.sness_spark",
    "utils"
]:
    try:
        spec = importlib.util.find_spec(modulo)
        print(f"{modulo}: {'DISPONÍVEL' if spec else 'NÃO DISPONÍVEL'}")
    except Exception as e:
        print(f"{modulo}: ERRO - {type(e).__name__}: {e}")

# COMMAND ----------

# IDENTIFICANDO VERSÕES NATIVAS DO DATABRICKS
import sys
import importlib.metadata as md

print("Python:", sys.version)
print("Spark:", spark.version)

pacotes = [
    "pyspark",
    "delta-spark",
    "pyarrow",
    "numpy",
    "setuptools",
    "wheel",
    "google-cloud-storage",
    "google-cloud-secret-manager",
    "boto3",
    "google-auth",
    "prometheus-client"
]

for pacote in pacotes:
    try:
        print(f"{pacote}: {md.version(pacote)}")
    except md.PackageNotFoundError:
        print(f"{pacote}: NÃO INSTALADO")

# COMMAND ----------

# IDENTIFICA O PATH do sness spark main e sness common main

current_user = spark.sql("SELECT current_user()").collect()[0][0]

base_path = f"/Workspace/Users/{current_user}/Magalu-visao-unica-precos/03_CONFIGURAÇÕES/BIBLIOTECA_SNESS"

sness_spark_path = f"{base_path}/sness-spark-main"
sness_common_path = f"{base_path}/sness-common-main"

print("Sness Spark:", sness_spark_path)
print("Sness Common:", sness_common_path)


# COMMAND ----------

# Instalar sness-lib-common-1.0.0 sem dependencias
%pip install --no-deps "{sness_common_path}"

# COMMAND ----------

# MAGIC %restart_python
# MAGIC print('Python reiniciado')

# COMMAND ----------

# Verifica versão do sness common instalada
import importlib.metadata as md

print("Sness Common:", md.version("sness-lib-common"))

# COMMAND ----------

# IDENTIFICA O PATH do sness spark main e sness common main

current_user = spark.sql("SELECT current_user()").collect()[0][0]

base_path = f"/Workspace/Users/{current_user}/Magalu-visao-unica-precos/03_CONFIGURAÇÕES/BIBLIOTECA_SNESS"

sness_spark_path = f"{base_path}/sness-spark-main"
sness_common_path = f"{base_path}/sness-common-main"

print("Sness Spark:", sness_spark_path)
print("Sness Common:", sness_common_path)

# COMMAND ----------

# DBTITLE 1,Instalar Sness Spark
# Instala Sness Spark 1.10.0
%pip install --no-deps "{sness_spark_path}"

# COMMAND ----------

# MAGIC %restart_python
# MAGIC print('Python reiniciado')

# COMMAND ----------

# IDENTIFICA O PATH do sness spark main e sness common main

current_user = spark.sql("SELECT current_user()").collect()[0][0]

base_path = f"/Workspace/Users/{current_user}/Magalu-visao-unica-precos/03_CONFIGURAÇÕES/BIBLIOTECA_SNESS"

sness_spark_path = f"{base_path}/sness-spark-main"
sness_common_path = f"{base_path}/sness-common-main"

print("Sness Spark:", sness_spark_path)
print("Sness Common:", sness_common_path)

# COMMAND ----------

# Verifica versão da instalação Sness Common e Sness Spark
import importlib.metadata as md
from pathlib import Path

try:
    import tomllib
except ImportError:
    tomllib = None

PACOTES = {
    "Sness Common": ("sness-lib-common", sness_common_path),
    "Sness Spark": ("sness-lib-spark", sness_spark_path)
}

def obter_versao(pacote, caminho):
    try:
        return md.version(pacote)
    except md.PackageNotFoundError:
        pass

    projeto = Path(caminho) / "pyproject.toml"

    if not projeto.is_file() or tomllib is None:
        return "Versão não identificada"

    try:
        with projeto.open("rb") as f:
            dados = tomllib.load(f)

        info = dados.get("project", {})
        if not info.get("version"):
            info = dados.get("tool", {}).get("poetry", {})

        return info.get("version") or "Versão não identificada"

    except Exception:
        return "Versão não identificada"


for nome, (pacote, caminho) in PACOTES.items():
    print(f"{nome}: {obter_versao(pacote, caminho)}")

# COMMAND ----------

# PACOTE SECRET MANAGER GCP NÃO INSTALADO
# REMOÇÃO DA NECESSIDADE do  SECRET MANAGER GCP NO ARQUIVO monitoring.py e secret.py
#%pip install "google-cloud-secret-manager~=2.20.1"
#/Workspace/Users/marco.vieira@marlabs.com/Magalu-visao-unica-precos/03_CONFIGURAÇÕES/BIBLIOTECA_SNESS/sness-common-main/sness/settings/secret.py
#/Workspace/Users/marco.vieira@marlabs.com/Magalu-visao-unica-precos/03_CONFIGURAÇÕES/BIBLIOTECA_SNESS/sness-spark-main/sness/datalake/monitoring.py

# COMMAND ----------

#CARREGAR BIBLIOTECAS SNESS, INICIALIZAR E IDENTIFICAR VERSOES
# ============================================================

import os
import sys
import importlib
import importlib.util
import importlib.metadata as md
import tomllib
from pathlib import Path

# ============================================================
# 1. IDENTIFICAR USUARIO E DIRETORIOS
# ============================================================

# IDENTIFICA O PATH do sness spark main e sness common main

current_user = spark.sql("SELECT current_user()").collect()[0][0]

base_path = f"/Workspace/Users/{current_user}/Magalu-visao-unica-precos/03_CONFIGURAÇÕES/BIBLIOTECA_SNESS"

sness_spark_path = f"{base_path}/sness-spark-main"
sness_common_path = f"{base_path}/sness-common-main"

current_user = spark.sql(
    "SELECT current_user()"
).first()[0]

# Variaveis criadas anteriormente
COMMON_ROOT = Path(sness_common_path)
SPARK_ROOT = Path(sness_spark_path)

# Diretorios internos dos pacotes Python
COMMON = COMMON_ROOT / "sness"
SPARK = SPARK_ROOT / "sness"

COMMON_DATALAKE = COMMON / "datalake"
SPARK_DATALAKE = SPARK / "datalake"

COMMON_STORAGE = COMMON / "storage"
SPARK_SETTINGS = SPARK / "settings"
SPARK_ETL = SPARK / "etl"

# ============================================================
# 2. VALIDAR DIRETORIOS
# ============================================================

obrigatorios = {
    "SNESS Common": COMMON,
    "SNESS Spark": SPARK,
    "Common metadata.py": COMMON_DATALAKE / "metadata.py",
    "Common storage": COMMON_STORAGE,
    "Spark sness_spark.py": SPARK_DATALAKE / "sness_spark.py",
    "Spark settings": SPARK_SETTINGS
}

for nome, caminho in obrigatorios.items():
    if not caminho.exists():
        raise FileNotFoundError(
            f"{nome} nao encontrado: {caminho}"
        )

    print(f"[OK] {nome}")

# ============================================================
# 3. CONFIGURAR AMBIENTE
# ============================================================

# Habilitar somente se as adaptacoes da SNESS para
# Serverless estiverem sendo utilizadas neste Compute.
# Esta flag nao altera o tipo real do Compute.
os.environ["SNESS_DATABRICKS_SERVERLESS"] = "true"

# ============================================================
# 4. CONFIGURAR NAMESPACE SNESS
# ============================================================

# Mantem o pacote instalado e adiciona as origens locais.
# Evita substituir o modulo sness em sys.modules.

import sness

for caminho in [COMMON, SPARK]:
    caminho_str = str(caminho)

    if caminho_str not in sness.__path__:
        sness.__path__.append(caminho_str)

# ============================================================
# 5. CONFIGURAR DATALAKE
# ============================================================

import sness.datalake

for caminho in [COMMON_DATALAKE, SPARK_DATALAKE]:
    caminho_str = str(caminho)

    if caminho_str not in sness.datalake.__path__:
        sness.datalake.__path__.append(caminho_str)

# ============================================================
# 6. CONFIGURAR ETL
# ============================================================

import sys
import importlib
from pathlib import Path

etl_path = Path(sness_spark_path) / "sness" / "etl"

if not etl_path.is_dir():
    raise FileNotFoundError(
        f"Diretorio SNESS ETL nao encontrado: {etl_path}"
    )

# Verifica se o modulo ja foi carregado
if "sness.etl" in sys.modules:
    etl = sys.modules["sness.etl"]
else:
    etl = importlib.import_module("sness.etl")

# Adiciona o caminho correto
if str(etl_path) not in etl.__path__:
    etl.__path__.append(str(etl_path))

print("[OK] SNESS ETL")
print("Caminho configurado:", etl_path)
print("Caminhos ativos:", list(etl.__path__))
print("Origem do modulo:", getattr(etl, "__file__", None))

# ============================================================
# 7. CONFIGURAR SETTINGS
# ============================================================

import importlib
from pathlib import Path

COMMON_SETTINGS = Path(sness_common_path) / "sness" / "settings"
SPARK_SETTINGS = Path(sness_spark_path) / "sness" / "settings"

# Valida a existência dos arquivos
cloud_config_file = COMMON_SETTINGS / "cloud_config.py"

if not cloud_config_file.is_file():
    raise FileNotFoundError(
        f"cloud_config.py não encontrado: {cloud_config_file}"
    )

# Carrega o pacote settings existente
import sness.settings

# Adiciona os diretórios das duas bibliotecas
for caminho in [COMMON_SETTINGS, SPARK_SETTINGS]:
    if caminho.is_dir():
        caminho_str = str(caminho)

        if caminho_str not in sness.settings.__path__:
            sness.settings.__path__.append(caminho_str)

importlib.invalidate_caches()

# Valida o módulo real da SNESS Common
from sness.settings import cloud_config

print("[OK] SNESS Settings")
print("cloud_config:", cloud_config.__file__)
print(
    "CLOUD_ENVIRONMENT:",
    getattr(cloud_config, "CLOUD_ENVIRONMENT", "Não definido")
)

# ============================================================
# 8. IMPORTAR COMPONENTES SNESS
# ============================================================

# Verifica primeiro a disponibilidade dos modulos
modulos = [
    "sness.datalake.metadata",
    "sness.storage",
    "sness.datalake.sness_spark"
]

print("\n===== MODULOS SNESS =====")

for nome in modulos:
    spec = importlib.util.find_spec(nome)

    if spec is None:
        raise ModuleNotFoundError(
            f"Modulo nao encontrado: {nome}"
        )

    print(f"[OK] {nome}")
    print(f"     {spec.origin}")

# Imports reais: podem revelar dependencias adicionais
from sness.datalake.metadata import Environment, Zone
from sness.datalake.sness_spark import SnessSpark

print("\n[OK] Environment e Zone")
print("[OK] SnessSpark")

# ============================================================
# 9. INICIALIZAR SNESS SPARK
# ============================================================

print("\nInicializando SNESS Spark...")

ss = SnessSpark()

if not hasattr(ss, "read"):
    raise RuntimeError(
        "SNESS inicializada sem o atributo read."
    )

if not hasattr(ss.read, "parquet"):
    raise RuntimeError(
        "SNESS inicializada sem o metodo read.parquet."
    )

print("SNESS Spark inicializada com sucesso.")

# ============================================================
# 10. IDENTIFICAR VERSOES
# ============================================================

def obter_versao_sness(diretorio, pacote):

    # Primeiro verifica a distribuicao instalada
    try:
        return md.version(pacote), "Instalada"
    except md.PackageNotFoundError:
        pass

    # Caso contrario, consulta o codigo-fonte
    arquivo = diretorio / "pyproject.toml"

    if not arquivo.is_file():
        return "Nao identificada", "Indisponivel"

    with arquivo.open("rb") as f:
        config = tomllib.load(f)

    versao = (
        config.get("project", {}).get("version")
        or config.get("tool", {})
                 .get("poetry", {})
                 .get("version")
        or "Nao identificada"
    )

    return versao, "Codigo-fonte"


versao_common, origem_common = obter_versao_sness(
    COMMON_ROOT,
    "sness-lib-common"
)

versao_spark, origem_spark = obter_versao_sness(
    SPARK_ROOT,
    "sness-lib-spark"
)

# ============================================================
# 11. VALIDAR INICIALIZACAO
# ============================================================

validacoes = {
    "Spark": spark is not None,
    "SNESS Common": COMMON.is_dir(),
    "SNESS Spark": SPARK.is_dir(),
    "Environment": Environment is not None,
    "Zone": Zone is not None,
    "SNESS ss": ss is not None,
    "Leitor Parquet": hasattr(ss.read, "parquet")
}

# ============================================================
# 12. RESULTADO FINAL
# ============================================================

print()
print("=" * 60)
print("BIBLIOTECAS SNESS - INICIALIZACAO")
print("=" * 60)

print(f"Sness Common : {versao_common} ({origem_common})")
print(f"Sness Spark  : {versao_spark} ({origem_spark})")
print(f"Apache Spark : {spark.version}")
print(f"Usuario      : {current_user}")
print(f"Classe ss    : {type(ss).__name__}")

print("-" * 60)

for componente, resultado in validacoes.items():
    status = "OK" if resultado else "ERRO"
    print(f"[{status}] {componente}")

print("=" * 60)

if all(validacoes.values()):
    print("SNESS INICIALIZADA E LEITOR DISPONIVEL")
else:
    raise RuntimeError(
        "Falha na validacao dos componentes SNESS."
    )


# COMMAND ----------

# Validação do Sness sem GCP Manager
import sys
import traceback
from pathlib import Path

try:
    # Valida os caminhos das bibliotecas
    for nome, caminho in {
        "Sness Common": sness_common_path,
        "Sness Spark": sness_spark_path
    }.items():
        if not Path(caminho).is_dir():
            raise FileNotFoundError(
                f"{nome}: diretório não encontrado: {caminho}"
            )

        # Adiciona o código-fonte ao Python Path
        if caminho not in sys.path:
            sys.path.insert(0, caminho)

        print(f"[OK] Caminho {nome}: {caminho}")

    # Valida Sness Common
    from sness.datalake.metadata import Environment, Zone
    print("[OK] Environment e Zone")

    # Valida Sness Spark
    from sness.datalake.sness_spark import SnessSpark
    print("[OK] SnessSpark")

    print("\nIMPORTS SNESS VALIDADOS COM SUCESSO")

except Exception:
    print("\n[ERRO] Falha na validação das bibliotecas SNESS")
    traceback.print_exc()

# COMMAND ----------

# remoção da necessidade PUSH_GATEWAY que enviava métricas de monitoramento para o ambiente Magalu/Luizalabs
# o Arquivo monitoring.py foi alterado

# COMMAND ----------

# Validação do ambiente sem PUSH_GATEWAY
from sness.datalake.metadata import Environment, Zone
from sness.datalake.sness_spark import SnessSpark
from sness.datalake.monitoring import Monitoring

print("[OK] Environment")
print("[OK] Zone")
print("[OK] SnessSpark")
print("[OK] Monitoring")

# COMMAND ----------

# Inicialização do SnessSpark foi alterada no arquivo sness-spark-main/sness/datalake/sness_spark.py, substituido somente o método __build_session para se adaptar a função nativa do Databricks

# COMMAND ----------

# Validando versão do Spark
from sness.datalake.sness_spark import SnessSpark

ss = SnessSpark()

print("[OK] SnessSpark inicializado")
print("Spark:", ss.spark.version)

# COMMAND ----------

# Alterado o caminho de leitura e escrita dos arquivos no arquivo sness-spark-main/sness/datalake/sness_spark.py, dentro da classe _SnessSparkBase.
# Leitura:	/Volumes/workspace/default/dados_teste_visao_unica/parquet/{dataset}
# Escrita:	/Volumes/workspace/default/dados_teste_visao_unica/resultados/{dataset}

# COMMAND ----------

# Preservado o comportamento do Databricks para ambientes Spark tradicionais e eliminando a configuração manual do Sness na função change_spark_config que manipula o ss.read dando erro de "spark.sql.catalog.spark_catalog is not available". a correção foi feita modificando apenas a função change_spark_config() no arquivo sness-spark-main/sness/settings/delta_config.py

# COMMAND ----------

# A função is_delta_lake() tenta obter o caminho pela API original do Magalu, mesmo depois de termos configurado o Sness para utilizar Databricks Volumes o que gera o erro KeyError: 'SNESS_PATH_API_URL'
# para corrigir foi alterada a função is_delta_lake() direcionado o path para Volumes Databricks - /Volumes/workspace/default/dados_teste_visao_unica/parquet/
#O arquivo que alterado foi: sness/datalake/delta_operations.py

# COMMAND ----------

# CRIE OS VOLUMES

# COMMAND ----------

# Validar criação de volumes
spark.sql("""
SHOW VOLUMES IN `test`.default
""").show(truncate=False)

# COMMAND ----------

# Preparar ambiente para Leitura dos arquivo
import os

os.environ["SNESS_DATABRICKS_SERVERLESS"] = "true"

assert os.environ.get("SNESS_DATABRICKS_SERVERLESS") == "true"

print("Ambiente SNESS configurado com sucesso.")
print("Spark:", spark.version)

# COMMAND ----------

# SUBA OS ARQUIVOS NO VOLUME
# RODE O NOTEBOOK NB_02_GERAR_BASES_ORIGINAIS_PARQUET PARA GERAR OS ARQUIVOS PARQUETS

# COMMAND ----------

# ============================================================
# VALIDAR DATASETS PARQUET NO NOVO VOLUME
# ============================================================

DESTINO_PARQUET = (
    "/Volumes/test/default/"
    "dados_origin_visao_unica/parquet"
)

datasets = {
    "base_prices_current": 5,
    "campaigns_v2": 50,
    "discounts_v2": 24,
    "payment_methods": 18,
    "prices": 5,
    "prices_historic": 56,
    "prices_penultimate_execution": 5,
    "products_discounts": 39,
    "robinhood_campaign_products": 22,
    "robinhood_campaigns": 13,
    "selling_prices2_current": 5,
    "visao_catalogo_produto_atual": 10
}

aprovados = 0
total = 0

for nome, esperado in datasets.items():
    caminho = f"{DESTINO_PARQUET}/{nome}"

    try:
        df = spark.read.parquet(caminho)
        quantidade = df.count()
        total += quantidade

        if quantidade == esperado:
            aprovados += 1
            status = "OK"
        else:
            status = "DIVERGENTE"

        print(
            f"[{status}] {nome}: "
            f"{quantidade} registros "
            f"(esperado: {esperado})"
        )

    except Exception as erro:
        mensagem = str(erro).splitlines()[0]
        print(f"[ERRO] {nome}: {mensagem}")

print("\n===== RESULTADO FINAL =====")
print(f"Datasets aprovados: {aprovados}/{len(datasets)}")
print(f"Total de registros: {total}/252")

# COMMAND ----------

# Criar o diretório de saída
destino = (
    "/Volumes/test/default/dados_origin_visao_unica/"
    "resultados"
)

dbutils.fs.mkdirs(destino)

print("===== DIRETÓRIO DE SAÍDA =====")
print("Caminho:", destino)
print("Conteúdo:", dbutils.fs.ls(destino))
print("[OK] Diretório preparado")

# COMMAND ----------

# Adaptação Writer.save()
# O código original do Writer.save() possui operações de GCP/Magalu que não devem ser executadas no Databricks Serverless.
# Portanto o método save() foi alterado no arquivo: sness-spark-main/sness/datalake/sness_spark.py

# COMMAND ----------

# Alterar caminhos dos volumes no arquivo pois não buscará mais da cloud /Workspace/Users/marco.vieira@marlabs.com/Magalu-visao-unica-precos/03_CONFIGURAÇÕES/BIBLIOTECA_SNESS/sness-spark-main/sness/datalake/sness_spark.py

# COMMAND ----------

# Reinicia o interpretador Python do notebook
# e limpa chach da memória

dbutils.library.restartPython()
print('Python reiniciado')

# COMMAND ----------


# RECARREGAR SNESS SPARK DO WORKSPACE SEM REINSTALAR
# ============================================================

import sys
import os
import importlib
import importlib.util
import importlib.machinery
from pathlib import Path

# ============================================================
# 1. IDENTIFICAR USUARIO E CAMINHOS
# ============================================================

current_user = spark.sql(
    "SELECT current_user()"
).first()[0]

base_path = (
    f"/Workspace/Users/{current_user}/"
    "Magalu-visao-unica-precos/"
    "03_CONFIGURAÇÕES/BIBLIOTECA_SNESS"
)

sness_common_path = f"{base_path}/sness-common-main"
sness_spark_path = f"{base_path}/sness-spark-main"

COMMON = Path(sness_common_path) / "sness"
SPARK = Path(sness_spark_path) / "sness"

COMMON_DATALAKE = COMMON / "datalake"
SPARK_DATALAKE = SPARK / "datalake"

COMMON_SETTINGS = COMMON / "settings"
SPARK_SETTINGS = SPARK / "settings"

ARQUIVO_SPARK = SPARK_DATALAKE / "sness_spark.py"

# ============================================================
# 2. VALIDAR ARQUIVOS
# ============================================================

arquivos = {
    "SNESS Common Metadata":
        COMMON_DATALAKE / "metadata.py",
    "SNESS Common Storage":
        COMMON / "storage" / "__init__.py",
    "SNESS Common Cloud Config":
        COMMON_SETTINGS / "cloud_config.py",
    "SNESS Spark":
        ARQUIVO_SPARK
}

for nome, arquivo in arquivos.items():
    if not arquivo.is_file():
        raise FileNotFoundError(
            f"{nome} nao encontrado: {arquivo}"
        )

    print(f"[OK] {nome}")

# ============================================================
# 3. CONFIGURAR AMBIENTE
# ============================================================

os.environ["SNESS_DATABRICKS_SERVERLESS"] = "true"

# ============================================================
# 4. PRIORIDADE DO PACOTE SNESS
# ============================================================

import sness

def priorizar_caminhos(pacote, caminhos):
    atuais = list(pacote.__path__)
    novos = [str(p) for p in caminhos if Path(p).is_dir()]

    pacote.__path__[:] = (
        novos +
        [p for p in atuais if p not in novos]
    )

priorizar_caminhos(
    sness,
    [SPARK, COMMON]
)

# ============================================================
# 5. PRIORIDADE DO DATALAKE
# ============================================================

import sness.datalake

priorizar_caminhos(
    sness.datalake,
    [SPARK_DATALAKE, COMMON_DATALAKE]
)

# ============================================================
# 6. PRIORIDADE DO SETTINGS
# ============================================================

import sness.settings

priorizar_caminhos(
    sness.settings,
    [COMMON_SETTINGS, SPARK_SETTINGS]
)

# ============================================================
# 7. ATUALIZAR CACHE
# ============================================================

importlib.invalidate_caches()

# ============================================================
# 8. VERIFICAR O ARQUIVO ANTES DE RECARREGAR
# ============================================================

spec = importlib.machinery.PathFinder.find_spec(
    "sness.datalake.sness_spark",
    sness.datalake.__path__
)

if spec is None or spec.origin is None:
    raise ModuleNotFoundError(
        "Nao foi possivel localizar sness_spark.py"
    )

arquivo_encontrado = Path(spec.origin).resolve()
arquivo_esperado = ARQUIVO_SPARK.resolve()

print("\n===== VALIDACAO DO CAMINHO =====")
print("Esperado  :", arquivo_esperado)
print("Encontrado:", arquivo_encontrado)

if arquivo_encontrado != arquivo_esperado:
    raise RuntimeError(
        "O Python ainda nao aponta para a SNESS Spark "
        "atualizada no Workspace."
    )

# ============================================================
# 9. RECARREGAR O MODULO
# ============================================================

nome_modulo = "sness.datalake.sness_spark"

modulo_anterior = sys.modules.get(nome_modulo)

if modulo_anterior is not None:
    # Atualiza a especificacao para o arquivo do Workspace
    modulo_anterior.__spec__ = spec
    modulo_anterior.__loader__ = spec.loader
    modulo_anterior.__file__ = spec.origin

    modulo_spark = importlib.reload(modulo_anterior)
else:
    modulo_spark = importlib.import_module(nome_modulo)

# ============================================================
# 10. VALIDAR A ORIGEM REAL
# ============================================================

origem_real = Path(modulo_spark.__file__).resolve()

print("\n===== SNESS SPARK RECARREGADA =====")
print("Arquivo:", origem_real)

if origem_real != arquivo_esperado:
    raise RuntimeError(
        "A SNESS Spark nao foi carregada do Workspace."
    )

# ============================================================
# 11. ATUALIZAR CLASSE E INSTANCIA
# ============================================================

SnessSpark = modulo_spark.SnessSpark

# Cria nova instancia usando a classe recarregada
ss = SnessSpark()

print("[OK] Classe SnessSpark atualizada")
print("[OK] Instancia ss recriada")

# ============================================================
# 12. RESULTADO FINAL
# ============================================================

print("\n" + "=" * 60)
print("RECARREGAMENTO SNESS SPARK")
print("=" * 60)

print("Usuario:", current_user)
print("Origem :", modulo_spark.__file__)
print("Classe :", SnessSpark.__name__)
print("Leitor :", hasattr(ss, "read"))
print("Writer :", hasattr(ss, "write"))

print("=" * 60)
print("[OK] RECARREGAMENTO CONCLUIDO")


# COMMAND ----------


# # Testar gravação no Databricks utilizando Writer.save()
import os
import traceback
from unittest.mock import patch

from sness.datalake.sness_spark import SnessSpark
from sness.datalake.metadata import Environment, Zone
from pyspark.sql.connect.readwriter import DataFrameWriter

os.environ["SNESS_DATABRICKS_SERVERLESS"] = "true"

ss = SnessSpark()

VOLUME = "/Volumes/test/default/dados_origin_visao_unica/resultados"
DATASET = "teste_save_sness_001"
CAMINHO_ESPERADO = f"{VOLUME}/{DATASET}"

print("===== TESTE DO METODO SAVE DA SNESS =====")

# 1. Criar DataFrame
df = ss.spark.createDataFrame(
    [
        (1, "Teste Databricks"),
        (2, "Teste Sness"),
        (3, "Teste Parquet")
    ],
    ["id", "descricao"]
)

print("1. DataFrame criado:", df.count(), "registros")

# 2. Capturar o caminho real sem alterar a implementacao
save_original = DataFrameWriter.save
caminhos_capturados = []

def save_monitorado(self, path=None, **kwargs):
    caminhos_capturados.append(path)
    print("2. Caminho recebido pelo Spark:", path)

    if path != CAMINHO_ESPERADO:
        raise ValueError(
            f"Caminho incorreto: {path}. "
            f"Esperado: {CAMINHO_ESPERADO}"
        )

    return save_original(self, path=path, **kwargs)

try:
    # 3. Executar o metodo SAVE real da SNESS
    with patch.object(DataFrameWriter, "save", save_monitorado):
        ss.write.save(
            df,
            environment=Environment.PRODUCTION,
            zone=Zone.TRUSTED,
            namespace="teste",
            dataset=DATASET,
            mode="errorifexists",
            format="parquet"
        )

    print("3. [OK] Metodo SAVE executado")

    # 4. Ler os dados gravados
    resultado = ss.spark.read.parquet(CAMINHO_ESPERADO)

    print("4. Registros gravados:", resultado.count())
    print("5. Colunas:", resultado.columns)
    print("6. Caminho:", CAMINHO_ESPERADO)

    assert resultado.count() == 3
    assert resultado.columns == ["id", "descricao"]

    print("\n[OK] TESTE SAVE SNESS APROVADO")

except Exception:
    print("\n[ERRO] TESTE SAVE SNESS REPROVADO")
    traceback.print_exc(limit=8)


# COMMAND ----------

# Correção no arquivo pyproject.toml que gerava recursão no empacotamento com mútilpas pastas a alteração foi realizada no arquivo: sness-spark-main/pyproject.toml

# COMMAND ----------

# Correção no arquivo utils.py preparando o config_spark() para o ambiente databricks

# COMMAND ----------

# Verifica a estrutura de pastas do Build
current_user = spark.sql("SELECT current_user()").collect()[0][0]
import os

raiz = (
    f"/Workspace/Users/{current_user}/"
    "Magalu-visao-unica-precos/"
    "03_CONFIGURAÇÕES/BIBLIOTECA_SNESS/sness-spark-main"
)

print("===== DIRETÓRIOS MAIS PROFUNDOS =====")

resultados = []

for pasta_atual, diretorios, arquivos in os.walk(raiz):
    profundidade = len(
        os.path.relpath(pasta_atual, raiz).split(os.sep)
    )

    resultados.append((profundidade, pasta_atual))

for profundidade, caminho in sorted(
    resultados, reverse=True
)[:15]:
    print(f"Nível {profundidade}: {caminho}")

print("\nTotal de diretórios:", len(resultados))

# COMMAND ----------

# ============================================================
# TESTE DE LEITURA SNESS SPARK - CODIGO ORIGINAL
# ============================================================

import os
import inspect
import traceback

from sness.datalake.sness_spark import SnessSpark
from sness.datalake.metadata import Environment, Zone

os.environ["SNESS_DATABRICKS_SERVERLESS"] = "true"

# ============================================================
# 1. CONFIGURACAO
# ============================================================

VOLUME_ORIGEM = "/Volumes/test/default/dados_origin_visao_unica"
VOLUME_RESULTADOS = "/Volumes/test/default/resultados"

DATASET = "teste_save_sness_001"

print("===== TESTE LEITURA SNESS =====")
print("Biblioteca:", inspect.getfile(SnessSpark))
print("Volume origem:", VOLUME_ORIGEM)
print("Volume resultados:", VOLUME_RESULTADOS)
print("Dataset:", DATASET)

# ============================================================
# 2. INICIALIZAR SNESS
# ============================================================

ss = SnessSpark()

# ============================================================
# 3. EXECUTAR METODO ORIGINAL
# ============================================================

try:

    df = ss.read.parquet(
        environment=Environment.PRODUCTION,
        zone=Zone.TRUSTED,
        namespace="teste",
        dataset=DATASET
    )

    print("\n1. Metodo ss.read.parquet executado")

    # Forca a leitura real
    quantidade = df.count()

    print("2. Registros:", quantidade)
    print("3. Colunas:", df.columns)

    assert quantidade == 3, (
        f"Esperados 3 registros, encontrados {quantidade}"
    )

    assert df.columns == ["id", "descricao"], (
        f"Colunas inesperadas: {df.columns}"
    )

    print("\n[OK] TESTE LEITURA SNESS APROVADO")

except Exception as erro:

    print("\n[ERRO] TESTE LEITURA SNESS REPROVADO")
    print("Tipo:", type(erro).__name__)
    print("Mensagem:", str(erro).split("JVM stacktrace:")[0])

    traceback.print_exc(limit=4)

# COMMAND ----------

# Inicializar os datasets de saída
base = "/Volumes/test/default/dados_origin_visao_unica"

datasets = [
    "prices",
    "prices_historic",
    "prices_penultimate_execution",
]

print("===== INICIALIZAÇÃO DOS DATASETS =====")

for dataset in datasets:
    origem = f"{base}/parquet/{dataset}"
    destino = f"{base}/resultados/{dataset}"

    if not dbutils.fs.ls(origem):
        raise RuntimeError(f"Origem vazia: {dataset}")

    try:
        dbutils.fs.ls(destino)
        print(f"[IGNORADO] Destino já existe: {dataset}")
        continue
    except Exception as erro:
        if "FileNotFoundException" not in str(erro) and \
           "PATH_NOT_FOUND" not in str(erro):
            raise

    print(f"Copiando: {dataset}")

    copiado = dbutils.fs.cp(
        origem,
        destino,
        recurse=True
    )

    if not copiado:
        raise RuntimeError(f"Falha ao copiar: {dataset}")

    print(f"[OK] {dataset}")

print("\n===== VALIDAÇÃO =====")

for dataset in datasets:
    origem = spark.read.parquet(f"{base}/parquet/{dataset}")
    destino = spark.read.parquet(f"{base}/resultados/{dataset}")

    qtd_origem = origem.count()
    qtd_destino = destino.count()

    print(
        f"{dataset}: "
        f"origem={qtd_origem}, "
        f"destino={qtd_destino}"
    )

    if qtd_origem != qtd_destino:
        raise RuntimeError(
            f"Divergência na cópia: {dataset}"
        )

# COMMAND ----------

# alteração do arquivo visao_precos.py devido as configurações do Spark no Databricks Serverless
# o Databricks somente permite 1 de 5 dessas alterações portanto foi preciso desabilitar essas alterações

# Permitida - spark.sql.shuffle.partitions
# Bloqueada - spark.sql.adaptive.enabled
# Bloqueada - spark.sql.adaptive.coalescePartitions.enabled
# Bloqueada - spark.default.parallelism
# Bloqueada - spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes

# COMMAND ----------

# Alteração do config_spark() no utils.py do arquivo: 01_CODIGO_AS_IS/visaounicapreco/visao_unica_commons/etls/utils/utils.py

# A alteração foi feita para que o Databricks utilize a sessão Spark existente, sem tentar configurar extensões Delta manualmente, evitando possíveis erros de incompatibilidade. No ambiente Magalu a configuração original do Spark e Delta será mantida.

# COMMAND ----------

# INSTALAÇÃO CONCLUDA