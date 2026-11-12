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

# Identificando o caminho do sness common main
current_user = spark.sql("SELECT current_user()").collect()[0][0]

lib_path = f"/Workspace/Users/{current_user}/PROJETO_VISAO_UNICA_PRECOS/03_CONFIGURAÇÕES/BIBLIOTECA_SNESS/sness-common-main"

print(lib_path)


# COMMAND ----------

# Instalar sness-lib-common-1.0.0 sem dependencias
%pip install --no-deps "{lib_path}"

# COMMAND ----------

# MAGIC %restart_python
# MAGIC print('Python reiniciado')

# COMMAND ----------

# Verifica versão do sness common instalada
import importlib.metadata as md

print("Sness Common:", md.version("sness-lib-common"))

# COMMAND ----------

# IDENTIFICA O PATH do sness spark main
current_user = spark.sql("SELECT current_user()").collect()[0][0]

lib_path = f"/Workspace/Users/{current_user}/PROJETO_VISAO_UNICA_PRECOS/03_CONFIGURAÇÕES/BIBLIOTECA_SNESS/sness-spark-main"

print(lib_path)

# COMMAND ----------

# DBTITLE 1,Instalar Sness Spark
# Instala Sness Spark 1.10.0
%pip install --no-deps "{lib_path}"

# COMMAND ----------

# MAGIC %restart_python
# MAGIC print('Python reiniciado')

# COMMAND ----------

# Verifica versão da instalação Sness Common e Sness Spark
import importlib.metadata as md
import importlib.util
import sys
import os
from pathlib import Path

try:
    import tomllib
except ImportError:
    tomllib = None

PACOTES = {
    "Sness Common": ("sness-lib-common", "metadata.py"),
    "Sness Spark": ("sness-lib-spark", "sness_spark.py")
}

def obter_versao(pacote, arquivo):
    try:
        return md.version(pacote)
    except md.PackageNotFoundError:
        pass

    if tomllib is None:
        return "Versão não identificada"

    raizes = [Path(p) for p in sys.path if p]
    raizes += [Path.cwd(), Path("/Workspace")]

    visitados = set()

    for raiz in raizes:
        if not raiz.is_dir():
            continue

        for atual, pastas, arquivos in os.walk(raiz):
            caminho = Path(atual)

            if str(caminho) in visitados:
                pastas[:] = []
                continue

            visitados.add(str(caminho))

            profundidade = len(caminho.parts) - len(raiz.parts)
            if profundidade > 8:
                pastas[:] = []
                continue

            pastas[:] = [
                p for p in pastas
                if p not in {".git", ".venv", "__pycache__", "node_modules"}
            ]

            if (
                caminho.name != "datalake"
                or caminho.parent.name != "sness"
                or arquivo not in arquivos
            ):
                continue

            for pasta in caminho.parents:
                projeto = pasta / "pyproject.toml"

                if projeto.is_file():
                    try:
                        with projeto.open("rb") as f:
                            dados = tomllib.load(f)

                        info = dados.get("project", {})
                        if not info.get("name"):
                            info = dados.get("tool", {}).get("poetry", {})

                        nome = str(info.get("name", "")).lower().replace("_", "-")

                        if nome == pacote:
                            return info.get("version", "Versão não declarada")
                    except Exception:
                        pass

                if pasta == raiz or pasta == Path("/"):
                    break

    return "Versão não identificada"


for nome, (pacote, arquivo) in PACOTES.items():
    print(f"{nome}: {obter_versao(pacote, arquivo)}")

# COMMAND ----------

# PACOTE SECRET MANAGER GCP NÃO INSTALADO
# REMOÇÃO DA NECESSIDADE NO ARQUIVO settings.py e secret.py
#%pip install "google-cloud-secret-manager~=2.20.1"

# COMMAND ----------


# Localiza os modulos SNESS no ambiente Python
# VALIDACAO SNESS - MULTIPLOS AMBIENTES DATABRICKS
# ============================================================
#
# Nao instala pacotes.
# Nao altera arquivos.
# Nao acessa GCP.
# Atualiza sys.path apenas na sessao atual.
# ============================================================

import sys
import os
import site
import importlib
import importlib.util
import traceback
from pathlib import Path


MODULOS = {
    "metadata": "sness.datalake.metadata",
    "spark": "sness.datalake.sness_spark"
}

ARQUIVOS = {
    "metadata": "metadata.py",
    "spark": "sness_spark.py"
}

# Limita a busca para evitar varrer todo o armazenamento
# do Workspace ou dos Volumes.
PROFUNDIDADE_MAXIMA = 7

# Pastas ignoradas durante a descoberta.
IGNORAR = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    ".cache",
    "node_modules",
    ".ipynb_checkpoints"
}


def diretorios_busca():
    """
    Identifica diretorios relevantes do ambiente atual.
    """

    candidatos = []

    # Diretorio atual
    candidatos.append(Path.cwd())

    # Diretorios do Python
    for caminho in sys.path:
        if caminho:
            candidatos.append(Path(caminho))

    # Bibliotecas instaladas
    try:
        candidatos.extend(
            Path(p) for p in site.getsitepackages()
        )
    except Exception:
        pass

    try:
        candidatos.append(Path(site.getusersitepackages()))
    except Exception:
        pass

    # Workspace Databricks
    candidatos.append(Path("/Workspace"))

    # Variavel opcional para projetos em local personalizado.
    # Exemplo:
    # os.environ["SNESS_PROJECT_ROOT"] = "/Workspace/Users/.../projeto"
    raiz_extra = os.environ.get("SNESS_PROJECT_ROOT")

    if raiz_extra:
        candidatos.append(Path(raiz_extra))

    resultado = []
    vistos = set()

    for caminho in candidatos:
        try:
            caminho = caminho.resolve()

            if caminho.is_dir() and str(caminho) not in vistos:
                resultado.append(caminho)
                vistos.add(str(caminho))

        except (OSError, RuntimeError):
            continue

    return resultado


def localizar_fontes():
    """
    Procura:
      sness/datalake/metadata.py
      sness/datalake/sness_spark.py

    Retorna os diretorios pais de 'sness'.
    """

    encontrados = {
        "metadata": set(),
        "spark": set()
    }

    for raiz in diretorios_busca():

        profundidade_raiz = len(raiz.parts)

        try:
            for atual, pastas, arquivos in os.walk(raiz):

                atual_path = Path(atual)

                profundidade = (
                    len(atual_path.parts) - profundidade_raiz
                )

                if profundidade >= PROFUNDIDADE_MAXIMA:
                    pastas[:] = []
                    continue

                pastas[:] = [
                    p for p in pastas
                    if p not in IGNORAR
                ]

                # So aceita a estrutura real do pacote.
                if (
                    atual_path.name != "datalake"
                    or atual_path.parent.name != "sness"
                ):
                    continue

                for chave, arquivo in ARQUIVOS.items():

                    if arquivo in arquivos:

                        raiz_pacote = atual_path.parent.parent

                        encontrados[chave].add(
                            str(raiz_pacote)
                        )

        except (OSError, PermissionError):
            continue

    return encontrados


def validar_imports():
    """
    Testa imports sem modificar o ambiente.
    """

    resultado = {}

    for chave, modulo in MODULOS.items():

        try:
            spec = importlib.util.find_spec(modulo)

            resultado[chave] = (
                spec is not None
                and spec.origin is not None
            )

        except (ImportError, ModuleNotFoundError):
            resultado[chave] = False

    return resultado


print("=" * 65)
print("DIAGNOSTICO SNESS - DATABRICKS")
print("=" * 65)

print("\n1. Verificando modulos existentes...")

estado = validar_imports()

for chave, encontrado in estado.items():
    print(
        f"{MODULOS[chave]}:",
        "ENCONTRADO" if encontrado else "NAO ENCONTRADO"
    )


# ============================================================
# DESCOBERTA AUTOMATICA
# ============================================================

if not all(estado.values()):

    print("\n2. Procurando bibliotecas SNESS...")

    encontrados = localizar_fontes()

    for chave, caminhos in encontrados.items():

        if caminhos:

            for caminho in sorted(caminhos):
                print(
                    f"[ENCONTRADO] {chave}: {caminho}"
                )

        else:
            print(
                f"[NAO ENCONTRADO] {chave}"
            )

    # Adiciona caminhos de codigo-fonte encontrados.
    # Nao modifica permanentemente o ambiente.
    novos_caminhos = sorted(
        set().union(*encontrados.values())
    )

    for caminho in novos_caminhos:

        if caminho not in sys.path:

            sys.path.append(caminho)

            print(
                "[SYS.PATH ADICIONADO]",
                caminho
            )

    importlib.invalidate_caches()

else:

    print(
        "\n2. Modulos ja disponiveis no ambiente."
    )


# ============================================================
# VALIDACAO FINAL
# ============================================================

print("\n3. Validando imports reais...")

try:

    from sness.datalake.metadata import Environment, Zone

    print("[OK] Environment e Zone")

    from sness.datalake.sness_spark import SnessSpark

    print("[OK] SnessSpark")

    import sness.datalake.metadata as metadata
    import sness.datalake.sness_spark as sness_spark

    print("\n4. Caminhos carregados:")

    print(
        "Metadata:",
        metadata.__file__
    )

    print(
        "SnessSpark:",
        sness_spark.__file__
    )

    print("\nIMPORTS SNESS VALIDADOS COM SUCESSO")

except Exception:

    print("\n[ERRO] Falha ao importar SNESS:")
    traceback.print_exc()

print("=" * 65)


# COMMAND ----------

# Validação do sness sem GCP Manager
try:
    from sness.datalake.metadata import Environment, Zone
    print("[OK] Environment e Zone")

    from sness.datalake.sness_spark import SnessSpark
    print("[OK] SnessSpark")

    print("\nIMPORTS SNESS VALIDADOS COM SUCESSO")

except Exception as e:
    import traceback
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

# ============================================================
# CRIAR VOLUME - VISAO UNICA DE PRECOS
# ============================================================
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS
    `demo-catalog`.visao_unica_precos
""")

print("Schema criado com sucesso.")

# COMMAND ----------

# Validar criação de volumes
spark.sql("""
SHOW VOLUMES IN `demo-catalog`.visao_unica_precos
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
    "/Volumes/demo-catalog/visao_unica_precos/"
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
    "/Volumes/demo-catalog/visao_unica_precos/"
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


# # Testar gravação no Databricks utilizando Writer.save()
import os
import traceback
from unittest.mock import patch

from sness.datalake.sness_spark import SnessSpark
from sness.datalake.metadata import Environment, Zone
from pyspark.sql.connect.readwriter import DataFrameWriter

os.environ["SNESS_DATABRICKS_SERVERLESS"] = "true"

ss = SnessSpark()

VOLUME = "/Volumes/demo-catalog/visao_unica_precos/resultados"
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

# Verifica a estrutura de pastas do Build
current_user = spark.sql("SELECT current_user()").collect()[0][0]
import os

raiz = (
    f"/Workspace/Users/{current_user}/"
    "PROJETO_VISAO_UNICA_PRECOS/"
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

# Testar Leitura dos resultados gravados no Databricks
import os
import traceback

os.environ["SNESS_DATABRICKS_SERVERLESS"] = "true"

from sness.datalake.sness_spark import SnessSpark
from sness.datalake.metadata import Environment, Zone

ss = SnessSpark()

try:
    df = ss.read.parquet(
        environment=Environment.PRODUCTION,
        zone=Zone.TRUSTED,
        namespace="teste",
        dataset="teste_escrita_sness"
    )

    print("1. Leitura pelo Sness: OK")
    print("2. Registros:", df.count())
    print("3. Colunas:", df.columns)

except Exception:
    traceback.print_exc(limit=8)

# COMMAND ----------

# Inicializar os datasets de saída
base = "/Volumes/demo-catalog/visao_unica_precos/dados_origin_visao_unica"

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