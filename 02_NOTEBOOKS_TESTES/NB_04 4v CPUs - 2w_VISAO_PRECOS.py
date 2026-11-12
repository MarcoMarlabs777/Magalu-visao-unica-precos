# Databricks notebook source
ORIGEM = '/Volumes/demo-catalog/visao_unica_precos/dados_origin_visao_unica/parquet'
VOLUME = "/Volumes/demo-catalog/visao_unica_precos/dados_origin_visao_unica/parquet_volumetria_final"

print("ORIGEM:", ORIGEM)
print("VOLUME:", VOLUME)

# COMMAND ----------


# ============================================================
# CARREGAR BIBLIOTECAS SNESS, INICIALIZAR E IDENTIFICAR VERSOES
# ============================================================

import os
import sys
import types
import importlib
import importlib.util
import tomllib

# ============================================================
# 1. IDENTIFICAR USUARIO E DIRETORIOS
# ============================================================

current_user = spark.sql(
    "SELECT current_user()"
).first()[0]

BASE = (
    f"/Workspace/Users/{current_user}/"
    "PROJETO_VISAO_UNICA_PRECOS"
)

COMMON_ROOT = (
    f"{BASE}/03_CONFIGURAÇÕES/"
    "BIBLIOTECA_SNESS/sness-common-main"
)

SPARK_ROOT = (
    f"{BASE}/03_CONFIGURAÇÕES/"
    "BIBLIOTECA_SNESS/sness-spark-main"
)

COMMON = f"{COMMON_ROOT}/sness"
SPARK = f"{SPARK_ROOT}/sness"

# ============================================================
# 2. VALIDAR DIRETORIOS
# ============================================================

for nome, caminho in [
    ("SNESS COMMON", COMMON),
    ("SNESS SPARK", SPARK)
]:
    if not os.path.isdir(caminho):
        raise FileNotFoundError(
            f"{nome} nao encontrado: {caminho}"
        )

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

sness = types.ModuleType("sness")
sness.__path__ = [COMMON, SPARK]

sys.modules["sness"] = sness

# ============================================================
# 5. CONFIGURAR DATALAKE
# ============================================================

datalake = types.ModuleType("sness.datalake")

datalake.__path__ = [
    f"{COMMON}/datalake",
    f"{SPARK}/datalake"
]

sys.modules["sness.datalake"] = datalake
sness.datalake = datalake

# ============================================================
# 6. CONFIGURAR ETL
# ============================================================

etl = types.ModuleType("sness.etl")

etl.__path__ = [
    p
    for p in [
        f"{COMMON}/etl",
        f"{SPARK}/etl"
    ]
    if os.path.isdir(p)
]

sys.modules["sness.etl"] = etl
sness.etl = etl

# ============================================================
# 7. CONFIGURAR SETTINGS
# ============================================================

settings_init = f"{SPARK}/settings/__init__.py"

if not os.path.isfile(settings_init):
    raise FileNotFoundError(
        f"Settings SNESS nao encontrado: {settings_init}"
    )

spec = importlib.util.spec_from_file_location(
    "sness.settings",
    settings_init,
    submodule_search_locations=[
        f"{COMMON}/settings",
        f"{SPARK}/settings"
    ]
)

settings = importlib.util.module_from_spec(spec)

sys.modules["sness.settings"] = settings
sness.settings = settings

spec.loader.exec_module(settings)

# ============================================================
# 8. IMPORTAR COMPONENTES SNESS
# ============================================================

from sness.datalake.metadata import (
    Zone,
    Environment
)

from sness.datalake.sness_spark import SnessSpark

# ============================================================
# 9. INICIALIZAR SNESS SPARK
# ============================================================

print("Inicializando SNESS Spark...")

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

def obter_versao_sness(diretorio):

    arquivo = os.path.join(
        diretorio,
        "pyproject.toml"
    )

    if not os.path.isfile(arquivo):
        return "Nao identificada"

    with open(arquivo, "rb") as f:
        config = tomllib.load(f)

    return (
        config.get("tool", {})
              .get("poetry", {})
              .get("version")
        or config.get("project", {})
                 .get("version")
        or "Nao identificada"
    )

versao_common = obter_versao_sness(COMMON_ROOT)
versao_spark = obter_versao_sness(SPARK_ROOT)

# ============================================================
# 11. VALIDAR INICIALIZACAO
# ============================================================

validacoes = {
    "Spark": spark is not None,
    "SNESS Common": os.path.isdir(COMMON),
    "SNESS Spark": os.path.isdir(SPARK),
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

print(f"Sness Common : {versao_common}")
print(f"Sness Spark  : {versao_spark}")
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


# ============================================================
# INVENTÁRIO DE DEPENDÊNCIAS - VISÃO ÚNICA PREÇOS
# Requirements | Pyproject | Ambiente Databricks
# ============================================================

import sys
import re
import importlib.util
import importlib.metadata as md
from pathlib import Path

try:
    import tomllib
except ImportError:
    import tomli as tomllib

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name


# ============================================================
# 1. CONFIGURAÇÃO DOS CAMINHOS
# ============================================================

PROJETO = Path(
    "/Workspace/Users/marco.vieira@marlabs.com/"
    "PROJETO_VISAO_UNICA_PRECOS"
)

REQUIREMENTS_PATH = (
    PROJETO
    / "01_CODIGO_AS_IS"
    / "visaounicapreco"
    / "visao_unica_preco"
    / "requirements.txt"
)

SNESS_BASE = (
    PROJETO
    / "03_CONFIGURAÇÕES"
    / "BIBLIOTECA_SNESS"
)

SPARK_ROOT = SNESS_BASE / "sness-spark-main"
COMMON_ROOT = SNESS_BASE / "sness-common-main"

PYPROJECT_PATH = SPARK_ROOT / "pyproject.toml"
COMMON_PYPROJECT = COMMON_ROOT / "pyproject.toml"


# ============================================================
# 2. PACOTES ADICIONAIS A INVENTARIAR
# ============================================================

PACOTES_ADICIONAIS = [
    "delta-spark",
    "pyarrow",
    "setuptools",
    "wheel",
    "boto3",
    "google-auth",
    "prometheus-client",
    "grpcio",
    "grpcio-status",
    "protobuf",
    "google-cloud-storage",
    "google-cloud-secret-manager",
]


# ============================================================
# 3. PACOTES PRIORITÁRIOS - SEMPRE NO FINAL
# ============================================================

PACOTES_PRIORITARIOS = [
    "google-cloud-storage",
    "google-cloud-secret-manager",
    "numpy",
    "pandas",
    "pyspark",
    "sness-lib",
    "sness-common",
    "sness-spark",
    "apache-spark (runtime)",
    "python",
]


# ============================================================
# 4. MÓDULOS PYTHON
# ============================================================

MODULOS = [
    "sness",
    "sness.datalake",
    "sness.datalake.metadata",
    "sness.datalake.sness_spark",
    "sness.etl.spark_utils",
    "utils",
]


# ============================================================
# 5. FUNÇÕES AUXILIARES
# ============================================================

def normalizar(nome):
    return canonicalize_name(nome)


def ler_requirements(caminho):
    """
    Lê as dependências diretamente do requirements.txt.
    Não instala nem altera pacotes.
    """
    dependencias = {}

    if not caminho.is_file():
        print(f"[AVISO] Requirements não encontrado: {caminho}")
        return dependencias

    for numero, linha in enumerate(
        caminho.read_text(encoding="utf-8-sig").splitlines(), 1
    ):
        linha = linha.strip()

        if not linha or linha.startswith("#"):
            continue

        linha = linha.split(" #", 1)[0].strip()

        try:
            req = Requirement(linha)
            dependencias[normalizar(req.name)] = (
                str(req.specifier) or "-"
            )
        except Exception:
            print(
                f"[AVISO] Requirements linha {numero} "
                f"não interpretada: {linha}"
            )

    return dependencias


def ler_pyproject(caminho):
    """
    Retorna nome, versão do projeto e dependências.
    """
    if not caminho.is_file():
        print(f"[AVISO] Pyproject não encontrado: {caminho}")
        return None, None, {}

    with caminho.open("rb") as arquivo:
        config = tomllib.load(arquivo)

    projeto = config.get("project", {})

    nome = projeto.get("name")
    versao = projeto.get("version")
    dependencias = {}

    for item in projeto.get("dependencies", []):
        try:
            req = Requirement(item)
            dependencias[normalizar(req.name)] = (
                str(req.specifier) or "-"
            )
        except Exception:
            print(f"[AVISO] Dependência não interpretada: {item}")

    python_requerido = projeto.get("requires-python")

    if python_requerido:
        dependencias["python"] = python_requerido

    return nome, versao, dependencias


def versao_instalada(pacote):
    try:
        return md.version(pacote)
    except md.PackageNotFoundError:
        return "NÃO INSTALADO"


def verificar_modulo(nome):
    if nome in sys.modules:
        return "DISPONÍVEL"

    try:
        return (
            "DISPONÍVEL"
            if importlib.util.find_spec(nome) is not None
            else "NÃO DISPONÍVEL"
        )
    except (ImportError, ModuleNotFoundError, ValueError):
        return "NÃO DISPONÍVEL"


def formatar_versao(valor):
    if valor is None or valor == "":
        return "-"
    return str(valor)


def imprimir_tabela(titulo, cabecalhos, linhas, larguras):
    largura_total = sum(larguras) + 3 * (len(larguras) - 1)

    print("\n" + "=" * largura_total)
    print(titulo)
    print("=" * largura_total)

    formato = "   ".join(
        f"{{:<{largura}}}" for largura in larguras
    )

    print(formato.format(*cabecalhos))
    print("-" * largura_total)

    for linha in linhas:
        print(formato.format(*map(str, linha)))

    print("-" * largura_total)
    print(f"TOTAL DE ITENS: {len(linhas)}")


# ============================================================
# 6. LEITURA DOS ARQUIVOS DO PROJETO
# ============================================================

requirements = ler_requirements(REQUIREMENTS_PATH)

nome_spark, versao_spark, deps_spark = ler_pyproject(
    PYPROJECT_PATH
)

nome_common, versao_common, deps_common = ler_pyproject(
    COMMON_PYPROJECT
)

# Consolidar dependências dos dois pyprojects,
# mantendo os requisitos do sness-spark como prioridade.
pyproject = dict(deps_common)
pyproject.update(deps_spark)

# Registrar versões dos projetos SNESS locais.
if versao_spark:
    pyproject["sness-spark"] = versao_spark

if versao_common:
    pyproject["sness-common"] = versao_common


# ============================================================
# 7. IDENTIFICAR PACOTES A COMPARAR
# ============================================================

pacotes = set(requirements)
pacotes.update(pyproject)
pacotes.update(map(normalizar, PACOTES_ADICIONAIS))

# Remover nomes especiais da lista convencional.
pacotes.discard("python")
pacotes.discard("apache-spark (runtime)")

# Ordenar os pacotes não prioritários alfabeticamente.
prioritarios_norm = {
    normalizar(nome) for nome in PACOTES_PRIORITARIOS
}

pacotes_comuns = sorted(
    pacote
    for pacote in pacotes
    if pacote not in prioritarios_norm
)


# ============================================================
# 8. VERSÕES DO AMBIENTE DATABRICKS
# ============================================================

def obter_spark_runtime():
    try:
        return spark.version
    except Exception:
        return "NÃO IDENTIFICADO"


def obter_versao_ambiente(pacote):
    """
    SNESS local pode estar carregada por código-fonte,
    mesmo sem uma distribuição instalada.
    """
    if pacote == "sness-common":
        instalado = versao_instalada("sness-lib-common")
        return (
            instalado if instalado != "NÃO INSTALADO"
            else (
                f"{versao_common} (código-fonte)"
                if versao_common else "NÃO INSTALADO"
            )
        )

    if pacote == "sness-spark":
        instalado = versao_instalada("sness-lib-spark")
        return (
            instalado if instalado != "NÃO INSTALADO"
            else (
                f"{versao_spark} (código-fonte)"
                if versao_spark else "NÃO INSTALADO"
            )
        )

    return versao_instalada(pacote)

# ============================================================
# 9. RESUMO DAS FONTES
# ============================================================

print("\n===== ARQUIVOS ANALISADOS =====")
print("Requirements :", REQUIREMENTS_PATH)
print("SNESS Spark  :", PYPROJECT_PATH)
print("SNESS Common :", COMMON_PYPROJECT)

print("\n===== VERSÕES DOS PROJETOS =====")
print("SNESS Spark  :", versao_spark or "NÃO IDENTIFICADA")
print("SNESS Common :", versao_common or "NÃO IDENTIFICADA")
print("Spark Runtime:", obter_spark_runtime())
print("Python       :", sys.version.split()[0])

# ============================================================
# 10. EXIBIR DISPONIBILIDADE DOS MÓDULOS
# ============================================================

linhas_modulos = [
    (modulo, verificar_modulo(modulo))
    for modulo in MODULOS
]

imprimir_tabela(
    titulo="DISPONIBILIDADE DOS MÓDULOS PYTHON",
    cabecalhos=["MÓDULO", "DISPONIBILIDADE"],
    linhas=linhas_modulos,
    larguras=[44, 24],
)

# ============================================================
# 11. MONTAR TABELA DE DEPENDÊNCIAS
# ============================================================

linhas = []

def adicionar_linha(pacote):
    linhas.append((
        pacote,
        formatar_versao(requirements.get(pacote)),
        formatar_versao(pyproject.get(pacote)),
        obter_versao_ambiente(pacote),
    ))


# Primeiro: demais pacotes.
for pacote in pacotes_comuns:
    adicionar_linha(pacote)

# Por último: os dez pacotes prioritários, na ordem solicitada.
for pacote in PACOTES_PRIORITARIOS:

    if pacote == "apache-spark (runtime)":
        linhas.append((
            pacote,
            formatar_versao(requirements.get("pyspark")),
            "-",
            obter_spark_runtime(),
        ))

    elif pacote == "python":
        linhas.append((
            pacote,
            "-",
            formatar_versao(pyproject.get("python")),
            sys.version.split()[0],
        ))

    else:
        adicionar_linha(normalizar(pacote))


# ============================================================
# 10. EXIBIR COMPARAÇÃO
# ============================================================

imprimir_tabela(
    titulo="DEPENDÊNCIAS - REQUIREMENTS X PYPROJECT X DATABRICKS",
    cabecalhos=[
        "PACOTE / MÓDULO",
        "REQUIREMENTS",
        "PYPROJECT",
        "DATABRICKS",
    ],
    linhas=linhas,
    larguras=[36, 19, 19, 29],
)








# COMMAND ----------

# ============================================================
# CONFIGURACAO DO MODULO UTILS
# ============================================================

import os
import sys
import importlib

current_user = spark.sql(
    "SELECT current_user()"
).collect()[0][0]

BASE = f"/Workspace/Users/{current_user}/PROJETO_VISAO_UNICA_PRECOS"

RAIZ_PROJETO = f"{BASE}/01_CODIGO_AS_IS/visaounicapreco"

DIRETORIO_UTILS = (
    f"{RAIZ_PROJETO}/visao_unica_commons/etls/utils"
)

ARQUIVO_UTILS = f"{DIRETORIO_UTILS}/utils.py"

print("===== CONFIGURACAO UTILS =====")

if not os.path.isfile(ARQUIVO_UTILS):
    raise FileNotFoundError(ARQUIVO_UTILS)

# Configurar caminhos
for caminho in [DIRETORIO_UTILS, RAIZ_PROJETO]:
    if caminho in sys.path:
        sys.path.remove(caminho)
    sys.path.insert(0, caminho)

# Atualizar cache de importacao
importlib.invalidate_caches()

# Importar modulo original
import utils

print("Status: OK")
print("Modulo:", utils.__name__)
print("Arquivo:", utils.__file__)

# COMMAND ----------

import sys

print("===== VALIDACAO UTILS =====")

assert "utils" in sys.modules, "Modulo utils nao carregado"

import utils

print("Status: OK")
print("Arquivo:", utils.__file__)

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "auto")

print(
    "Shuffle partitions:",
    spark.conf.get("spark.sql.shuffle.partitions")
)


# COMMAND ----------


# ============================================================
# EXECUCAO MONITORADA - VISAO UNICA DE PRECOS
# DATABRICKS - 4v CPUs - 2w
# ============================================================

import os
import sys
import time
import inspect
import traceback
import importlib.util
from datetime import datetime
from functools import wraps

os.environ["SNESS_DATABRICKS_SERVERLESS"] = "true"

# ============================================================
# 1. CONFIGURACOES
# ============================================================

current_user = spark.sql(
    "SELECT current_user()"
).collect()[0][0]

BASE = f"/Workspace/Users/{current_user}/PROJETO_VISAO_UNICA_PRECOS"

RAIZ_PROJETO = f"{BASE}/01_CODIGO_AS_IS/visaounicapreco"

ARQUIVO = (
    f"{RAIZ_PROJETO}/visao_unica_preco/"
    "etls/trusted/visao_precos.py"
)

VOLUME = "/Volumes/demo-catalog/visao_unica_precos/dados_origin_visao_unica/parquet_volumetria_final"

AMBIENTE = "prd"

# True: calcula count() real de cada base.
# False: usa quantidades de referencia, sem ler os Parquets.
CONTAR_REGISTROS_REAIS = False

# Quantidades de referencia da volumetria gerada anteriormente.
# Nao representam contagens verificadas nesta execucao.
VOLUMETRIA_REFERENCIA = {
    "base_prices_current": 209630280,
    "products_discounts": 3780819,
    "selling_prices2_current": 236959643,
    "discounts_v2": 28836725,
    "robinhood_campaigns": 4381282,
    "robinhood_campaign_products": 108024399,
    "campaigns_v2": 474365106,
    "payment_methods": 18,
    "visao_catalogo_produto_atual": 511653317,
    "prices": 209630280,
    "prices_historic": 628890840,
    "prices_penultimate_execution": 209630280
}

# ============================================================
# 2. FUNCOES AUXILIARES
# ============================================================

def agora():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def formatar_tempo(segundos):
    horas = int(segundos // 3600)
    minutos = int((segundos % 3600) // 60)
    segs = segundos % 60
    return f"{horas:02d}:{minutos:02d}:{segs:06.3f}"


def formatar_numero(valor):
    return f"{valor:,}".replace(",", ".")


def log(mensagem):
    print(f"[{agora()}] {mensagem}", flush=True)


# ============================================================
# 3. INICIO
# ============================================================

inicio_data = datetime.now()
inicio_tempo = time.perf_counter()

print("=" * 78)
print("                     VISAO UNICA DE PRECOS")
print("                       EXECUCAO COMPLETA")
print("=" * 78)

print(f"Script: {ARQUIVO}")
print(f"Ambiente informado: {AMBIENTE}")
print(f"Inicio: {inicio_data:%Y-%m-%d %H:%M:%S}")
print(f"Spark: {spark.version}")
print(f"Volume de testes: {VOLUME}")

# ============================================================
# 4. CONFIGURAR IMPORTACOES
# ============================================================

if not os.path.isfile(ARQUIVO):
    raise FileNotFoundError(ARQUIVO)

if RAIZ_PROJETO not in sys.path:
    sys.path.insert(0, RAIZ_PROJETO)

utils_encontrados = []

for raiz, _, arquivos in os.walk(RAIZ_PROJETO):
    if "utils.py" in arquivos:
        utils_encontrados.append(os.path.join(raiz, "utils.py"))

if len(utils_encontrados) != 1:
    raise RuntimeError(
        f"Esperado exatamente um utils.py: {utils_encontrados}"
    )

diretorio_utils = os.path.dirname(utils_encontrados[0])

if diretorio_utils not in sys.path:
    sys.path.insert(0, diretorio_utils)

# Preservar os modulos SNESS e utils ja carregados.

# ============================================================
# 5. INVENTARIO DAS BASES
# ============================================================

print("\n" + "=" * 78)
print("                     BASES DE DADOS")
print("=" * 78)

contagens = {}
erros_contagem = {}

for nome in VOLUMETRIA_REFERENCIA:

    caminho = f"{VOLUME}/{nome}"

    if not os.path.isdir(caminho):
        log(f"BASE NAO ENCONTRADA: {nome}")
        erros_contagem[nome] = "Diretorio nao encontrado"
        continue

    if CONTAR_REGISTROS_REAIS:

        log(f"Contando registros: {nome}")
        inicio_contagem = time.perf_counter()

        try:
            quantidade = spark.read.parquet(caminho).count()
            contagens[nome] = quantidade

            log(
                f"{nome}: {formatar_numero(quantidade)} "
                f"registros | "
                f"{formatar_tempo(time.perf_counter() - inicio_contagem)}"
            )

        except Exception as erro:
            erros_contagem[nome] = str(erro)
            log(f"ERRO CONTAGEM {nome}: {erro}")

    else:

        quantidade = VOLUMETRIA_REFERENCIA[nome]
        contagens[nome] = quantidade

        print(
            f"{nome:<38} "
            f"{formatar_numero(quantidade):>18} registros"
        )

total_registros = sum(contagens.values())

print("-" * 78)
print(
    f"{'TOTAL DAS BASES LISTADAS':<38} "
    f"{formatar_numero(total_registros):>18} registros"
)

print(
    "Tipo de contagem:",
    "REAL" if CONTAR_REGISTROS_REAIS else "REFERENCIA"
)

# ============================================================
# 6. CARREGAR SCRIPT ORIGINAL
# ============================================================

log("Carregando visao_precos.py")

spec = importlib.util.spec_from_file_location(
    "visao_precos_original",
    ARQUIVO
)

modulo_visao_precos = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = modulo_visao_precos
spec.loader.exec_module(modulo_visao_precos)

if not callable(getattr(modulo_visao_precos, "main", None)):
    raise RuntimeError("Funcao main() nao encontrada")

log("Script carregado com sucesso")

# ============================================================
# 7. MONITORAMENTO DAS FUNCOES
# ============================================================

funcoes_pipeline = {}

for nome, funcao in inspect.getmembers(
    modulo_visao_precos,
    inspect.isfunction
):
    if (
        funcao.__module__ == modulo_visao_precos.__name__
        and nome != "main"
    ):
        funcoes_pipeline[nome] = funcao

estatisticas = {}
pilha_execucao = []

def criar_monitor(nome, funcao_original):

    @wraps(funcao_original)
    def monitor(*args, **kwargs):

        inicio = time.perf_counter()

        dados = estatisticas.setdefault(
            nome,
            {
                "chamadas": 0,
                "sucessos": 0,
                "erros": 0,
                "tempo_total": 0.0
            }
        )

        dados["chamadas"] += 1
        nivel = len(pilha_execucao)
        pilha_execucao.append(nome)

        prefixo = "  " * nivel

        log(f"{prefixo}INICIO | {nome}")

        try:
            resultado = funcao_original(*args, **kwargs)

            dados["sucessos"] += 1

            log(
                f"{prefixo}FIM    | {nome} | "
                f"{formatar_tempo(time.perf_counter() - inicio)}"
            )

            return resultado

        except Exception as erro:

            dados["erros"] += 1

            log(
                f"{prefixo}ERRO   | {nome}: "
                f"{type(erro).__name__}: {erro}"
            )

            raise

        finally:

            dados["tempo_total"] += (
                time.perf_counter() - inicio
            )

            pilha_execucao.pop()

    return monitor


for nome, funcao in funcoes_pipeline.items():

    setattr(
        modulo_visao_precos,
        nome,
        criar_monitor(nome, funcao)
    )

log(f"Funcoes monitoradas: {len(funcoes_pipeline)}")

# ============================================================
# 8. EXECUTAR PIPELINE
# ============================================================

print("\n" + "=" * 78)
print("                       PROCESSAMENTO")
print("=" * 78)

resultado = None
erro_execucao = None

try:

    log("Iniciando main()")

    resultado = modulo_visao_precos.main(
        ["--environment", AMBIENTE]
    )

    log("main() retornou sem excecao")

except Exception as erro:

    erro_execucao = erro

    log(
        f"ERRO: {type(erro).__name__}: {erro}"
    )

    traceback.print_exc(limit=20)

finally:

    # ========================================================
    # 9. RELATORIO FINAL
    # ========================================================

    fim_data = datetime.now()
    tempo_total = time.perf_counter() - inicio_tempo

    print("\n" + "=" * 78)
    print("                       RELATORIO FINAL")
    print("=" * 78)

    print("\n===== REGISTROS DAS BASES =====")

    for nome, quantidade in contagens.items():

        print(
            f"{nome:<38} "
            f"{formatar_numero(quantidade):>18}"
        )

    print("-" * 78)

    print(
        f"{'TOTAL DAS BASES LISTADAS':<38} "
        f"{formatar_numero(total_registros):>18}"
    )

    print(
        "Contagens:",
        "Reais" if CONTAR_REGISTROS_REAIS else "Referencia"
    )

    if erros_contagem:
        print(
            f"Bases sem contagem: {len(erros_contagem)}"
        )

    print("\n===== FUNCOES EXECUTADAS =====")

    if estatisticas:

        for nome, dados in estatisticas.items():

            print(
                f"{nome:<42} "
                f"Chamadas: {dados['chamadas']:<3} "
                f"Erros: {dados['erros']:<3} "
                f"Tempo: {formatar_tempo(dados['tempo_total'])}"
            )

    else:
        print("Nenhuma funcao monitorada foi chamada.")

    print("\n===== RESULTADO =====")

    if erro_execucao is None:
        print("STATUS: MAIN RETORNOU SEM EXCECAO")
    else:
        print("STATUS: ERRO")

    print(f"Retorno: {resultado!r}")

    print("\n===== TEMPO DE EXECUCAO =====")

    print(f"Inicio:         {inicio_data:%Y-%m-%d %H:%M:%S}")
    print(f"Termino:        {fim_data:%Y-%m-%d %H:%M:%S}")
    print(f"Tempo total:    {formatar_tempo(tempo_total)}")
    print(f"Total segundos: {tempo_total:,.3f}")

    print("=" * 78)

if erro_execucao is not None:
    raise erro_execucao

# COMMAND ----------

configs = [
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
    "spark.sql.files.maxPartitionBytes",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.adaptive.autoBroadcastJoinThreshold"
]

for config in configs:
    try:
        print(f"{config} = {spark.conf.get(config)}")
    except Exception as e:
        print(f"{config} = INDISPONÍVEL ({type(e).__name__})")