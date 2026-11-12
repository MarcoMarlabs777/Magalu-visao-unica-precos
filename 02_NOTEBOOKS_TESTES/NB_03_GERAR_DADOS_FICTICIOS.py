# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
ORIGEM = '/Volumes/demo-catalog/visao_unica_precos/dados_origin_visao_unica/parquet'
DESTINO = "/Volumes/demo-catalog/visao_unica_precos/dados_origin_visao_unica/parquet_volumetria_final"


EXECUTAR_GRAVACAO = True
MODO_AMOSTRA = False
LIMITE_AMOSTRA = 628890840

# Volumetria Alvo para cada base
ALVOS = {
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

print("ORIGEM:", ORIGEM)
print("DESTINO:", DESTINO)

# COMMAND ----------


# Databricks Serverless - Gerador V5
# Volumetria com referencias sinteticas consistentes.
#
# EXECUTE primeiro a celula de configuracao.
# Este script nao redefine ORIGEM/DESTINO/ALVOS.
#
# PARTICIONAMENTO = False (padrao)
# False: usa o particionamento interno do Spark.
# True: controla as particoes na geracao e na gravacao.

from pyspark.sql import functions as F, types as T
import math


# ============================================================
# 1. VALIDACAO DAS CONFIGURACOES
# ============================================================

for variavel in (
    'ORIGEM',
    'DESTINO',
    'ALVOS',
    'MODO_AMOSTRA',
    'LIMITE_AMOSTRA',
    'EXECUTAR_GRAVACAO'
):
    if variavel not in globals():
        raise ValueError(
            f'Defina {variavel} na celula de configuracao '
            f'antes de executar.'
        )

if (
    ORIGEM.rstrip('/') == DESTINO.rstrip('/')
    or DESTINO.rstrip('/').startswith(
        ORIGEM.rstrip('/') + '/'
    )
):
    raise ValueError(
        'DESTINO nao pode ser a ORIGEM '
        'nem uma subpasta da ORIGEM'
    )

if (
    isinstance(LIMITE_AMOSTRA, bool)
    or not isinstance(LIMITE_AMOSTRA, int)
    or LIMITE_AMOSTRA < 1
):
    raise ValueError(
        'LIMITE_AMOSTRA precisa ser inteiro positivo'
    )

if any(
    type(v) is not int or v < 0
    for v in ALVOS.values()
):
    raise ValueError(
        'ALVOS precisa conter inteiros nao negativos'
    )


# ============================================================
# 2. CONFIGURACAO DE PARTICIONAMENTO
# ============================================================

# False = nao define explicitamente o numero de particoes.
# True  = aplica particionamento na geracao e na gravacao.
#
# Pode ser definido na celula de configuracao.
# Se nao existir, assume False.

PARTICIONAMENTO = globals().get(
    'PARTICIONAMENTO',
    False
)

# Quantidade desejada de linhas por particao.
# Utilizada somente com PARTICIONAMENTO = True.

LINHAS_POR_PARTICAO = globals().get(
    'LINHAS_POR_PARTICAO',
    1_000_000
)

# Limite maximo de particoes.
# Utilizado somente com PARTICIONAMENTO = True.

MAX_PARTICOES = globals().get(
    'MAX_PARTICOES',
    4096
)

DATA_REFERENCIA = globals().get(
    'DATA_REFERENCIA',
    '2026-11-10'
)

if not isinstance(PARTICIONAMENTO, bool):
    raise TypeError(
        'PARTICIONAMENTO deve ser True ou False'
    )

if PARTICIONAMENTO:
    if (
        type(LINHAS_POR_PARTICAO) is not int
        or LINHAS_POR_PARTICAO < 1
    ):
        raise ValueError(
            'LINHAS_POR_PARTICAO deve ser inteiro positivo'
        )

    if (
        type(MAX_PARTICOES) is not int
        or MAX_PARTICOES < 1
    ):
        raise ValueError(
            'MAX_PARTICOES deve ser inteiro positivo'
        )


def calcular_particoes(quantidade):
    """
    Calcula a quantidade de particoes somente
    quando o particionamento esta habilitado.
    """

    return max(
        1,
        min(
            MAX_PARTICOES,
            math.ceil(
                quantidade / LINHAS_POR_PARTICAO
            )
        )
    )


print('=' * 65)
print('CONFIGURACAO GERADOR V5')
print('=' * 65)

print('ORIGEM:', ORIGEM)
print('DESTINO:', DESTINO)

print(
    'PARTICIONAMENTO:',
    'ATIVADO' if PARTICIONAMENTO else 'DESATIVADO'
)

if PARTICIONAMENTO:
    print(
        'LINHAS_POR_PARTICAO:',
        f'{LINHAS_POR_PARTICAO:,}'
    )
    print('MAX_PARTICOES:', MAX_PARTICOES)

print('=' * 65)


# ============================================================
# 3. LEITURA DAS BASES ORIGINAIS
# ============================================================

originais = {
    n: spark.read.parquet(f'{ORIGEM}/{n}')
    for n in ALVOS
}

contagens = {
    n: d.count()
    for n, d in originais.items()
}

metas = {
    n: min(v, LIMITE_AMOSTRA) if MODO_AMOSTRA else v
    for n, v in ALVOS.items()
}

qtd_syn = {
    n: metas[n] - contagens[n]
    for n in ALVOS
}

for n in ALVOS:
    if qtd_syn[n] < 0:
        raise ValueError(
            f'{n}: meta {metas[n]} '
            f'menor que originais {contagens[n]}'
        )


# ============================================================
# 4. VALIDACAO DOS RELACIONAMENTOS
# ============================================================

# Relacoes exigem que a tabela pai tenha
# pelo menos uma linha sintetica.

relacoes = {
    'selling_prices2_current': 'base_prices_current',
    'prices': 'base_prices_current',
    'prices_historic': 'base_prices_current',
    'prices_penultimate_execution': 'base_prices_current',
    'visao_catalogo_produto_atual': 'base_prices_current',
    'products_discounts': 'discounts_v2',
    'robinhood_campaign_products': 'robinhood_campaigns',
}

for filho, pai in relacoes.items():

    if (
        filho in qtd_syn
        and pai in qtd_syn
        and qtd_syn[filho] > 0
        and qtd_syn[pai] <= 0
    ):
        raise ValueError(
            f'{filho} requer linhas sinteticas em {pai}'
        )


# ============================================================
# 5. PLANEJAMENTO
# ============================================================

print()
print('PLANEJAMENTO V5')

for n in ALVOS:

    print(
        f'{n:36} '
        f'original={contagens[n]:>12,} '
        f'alvo_execucao={metas[n]:>12,} '
        f'sinteticos={qtd_syn[n]:>12,}'
    )

print(
    'TOTAL:',
    f'{sum(metas.values()):,}',
    '| GRAVACAO:',
    EXECUTAR_GRAVACAO
)

print()


# ============================================================
# 6. FUNCOES AUXILIARES
# ============================================================

def id_logico(nome, indice):

    return F.substring(
        F.sha2(
            F.concat(
                F.lit('V5|' + nome + '|'),
                indice.cast('string')
            ),
            256
        ),
        1,
        32
    )


def campo(df, nome, valor):

    if nome not in df.columns:
        return df

    tipo = df.schema[nome].dataType

    if isinstance(tipo, T.ArrayType):

        if isinstance(
            tipo.elementType,
            T.StringType
        ):
            valor = F.array(
                valor.cast('string')
            )
        else:
            return df

    elif isinstance(
        tipo,
        (T.StructType, T.MapType)
    ):
        return df

    return df.withColumn(
        nome,
        valor.cast(tipo)
    )


# ============================================================
# 7. GERACAO DE REGISTROS SINTETICOS
# ============================================================

def sinteticos(nome):

    quantidade = qtd_syn[nome]
    base = originais[nome]

    if quantidade == 0:
        return base.limit(0)

    # --------------------------------------------------------
    # PARTICIONAMENTO OPCIONAL NA GERACAO
    # --------------------------------------------------------

    if PARTICIONAMENTO:

        particoes = calcular_particoes(quantidade)

        seq = spark.range(
            0,
            quantidade,
            1,
            numPartitions=particoes
        ).withColumnRenamed(
            'id',
            '__seq'
        )

    else:

        # Spark determina o particionamento interno.
        seq = spark.range(
            0,
            quantidade,
            1
        ).withColumnRenamed(
            'id',
            '__seq'
        )

    # --------------------------------------------------------
    # GERACAO DOS DADOS
    # --------------------------------------------------------

    df = base.limit(1).crossJoin(seq)

    idx = F.col('__seq')

    n_prod = qtd_syn['base_prices_current']
    n_desc = qtd_syn['discounts_v2']
    n_robin = qtd_syn['robinhood_campaigns']

    prod_idx = (
        F.pmod(idx, F.lit(n_prod))
        if n_prod > 0
        else F.lit(0)
    )

    desc_idx = (
        F.pmod(idx, F.lit(n_desc))
        if n_desc > 0
        else F.lit(0)
    )

    robin_idx = (
        F.pmod(idx, F.lit(n_robin))
        if n_robin > 0
        else F.lit(0)
    )

    seller = id_logico(
        'seller',
        F.floor(
            prod_idx / F.lit(5)
        )
    )

    sku = id_logico(
        'sku',
        prod_idx
    )

    for coluna, valor in [
        ('seller_id', seller),
        ('seller', seller),
        ('sku', sku),
        (
            'navigation_id',
            id_logico('nav', prod_idx)
        ),
        (
            'id',
            id_logico(nome, idx)
        )
    ]:
        df = campo(
            df,
            coluna,
            valor
        )

    if nome == 'discounts_v2':

        df = campo(
            df,
            'id',
            id_logico('discounts_v2', idx)
        )

    if nome == 'products_discounts':

        df = campo(
            df,
            'campaign_id',
            id_logico(
                'discounts_v2',
                desc_idx
            )
        )

    if nome == 'robinhood_campaigns':

        df = campo(
            df,
            'campaign_id',
            id_logico(
                'robinhood_campaigns',
                idx
            )
        )

    if nome == 'robinhood_campaign_products':

        df = campo(
            df,
            'campaign_id',
            id_logico(
                'robinhood_campaigns',
                robin_idx
            )
        )

    df = campo(
        df,
        'campaign_code',
        F.concat(
            F.lit('V5-'),
            idx.cast('string')
        )
    )

    for coluna, valor in [
        ('op', 'c'),
        ('action', 'insert'),
        ('currency', 'BRL'),
        ('category_id', 'CATEGORIA_V5'),
        ('brand', 'MARCA_V5'),
        ('payment_method', 'pix'),
        ('status', 'active'),
        ('benefit', 'loyalty'),
        ('type', 'sku'),
        ('channel_id', '*')
    ]:

        df = campo(
            df,
            coluna,
            F.lit(valor)
        )

    for coluna in (
        'is_deleted',
        'active',
        'stock'
    ):

        df = campo(
            df,
            coluna,
            F.lit(
                coluna != 'is_deleted'
            )
        )

    for coluna, valor in [
        ('price', 100.0),
        ('price_por', 100.0),
        ('price_de', 150.0),
        ('list_price', 150.0)
    ]:

        df = campo(
            df,
            coluna,
            F.lit(valor)
            + F.pmod(
                idx,
                F.lit(10000)
            ) / F.lit(100.0)
        )

    for coluna, data in [
        ('start_date', '2026-11-08'),
        ('end_date', '2027-02-08'),
        ('start_at', '2026-11-08'),
        ('end_at', '2027-02-08')
    ]:

        df = campo(
            df,
            coluna,
            F.lit(data)
        )

    for coluna in (
        'created_at',
        'updated_at',
        'timestamp_kafka',
        'checkpoint_timestamp',
        'price_timestamp'
    ):

        df = campo(
            df,
            coluna,
            F.lit(DATA_REFERENCIA)
        )

    for coluna in (
        'timestamp_kafka_day',
        'price_date'
    ):

        valor = (
            F.date_sub(
                F.lit(DATA_REFERENCIA),
                F.pmod(
                    idx,
                    F.lit(90)
                ).cast('int')
            )
            if nome == 'prices_historic'
            else F.lit(DATA_REFERENCIA)
        )

        df = campo(
            df,
            coluna,
            valor
        )

    df = campo(
        df,
        'deleted_at',
        F.lit(None)
    )

    return df.select(
        *base.columns
    )


# ============================================================
# 8. GRAVACAO DOS ARQUIVOS PARQUET
# ============================================================

if EXECUTAR_GRAVACAO:

    # --------------------------------------------------------
    # VERIFICACAO DO DESTINO
    # --------------------------------------------------------

    try:
        dbutils.fs.ls(DESTINO)

    except Exception as exc:

        if not any(
            t in str(exc).lower()
            for t in (
                'not found',
                'does not exist',
                'filenotfound',
                'path_not_found'
            )
        ):
            raise

    else:
        raise RuntimeError(
            f'DESTINO JA EXISTE: {DESTINO}; '
            f'escolha outro destino'
        )

    # --------------------------------------------------------
    # PROCESSAMENTO DAS TABELAS
    # --------------------------------------------------------

    for nome in ALVOS:

        print(
            f'GERANDO {nome}: '
            f'{qtd_syn[nome]:,} sinteticos'
        )

        if qtd_syn[nome] == 0:

            dados = originais[nome]

        else:

            dados = originais[nome].unionByName(
                sinteticos(nome)
            )

        # ----------------------------------------------------
        # PARTICIONAMENTO OPCIONAL NA GRAVACAO
        # ----------------------------------------------------

        if PARTICIONAMENTO:

            particoes = calcular_particoes(
                metas[nome]
            )

            dados = dados.repartition(
                particoes
            )

            print(
                f'PARTICIONAMENTO | {nome} | '
                f'{particoes} particoes'
            )

        else:

            print(
                f'SEM REPARTITION EXPLICITO | {nome}'
            )

        # ----------------------------------------------------
        # GRAVACAO PARQUET
        # ----------------------------------------------------

        dados.write.mode(
            'overwrite'
        ).parquet(
            f'{DESTINO}/{nome}'
        )

        print(
            'OK',
            nome
        )

    print()
    print('=' * 65)
    print('PROCESSAMENTO FINALIZADO')
    print('=' * 65)

    print('DESTINO:', DESTINO)

    print(
        'PARTICIONAMENTO:',
        'ATIVADO' if PARTICIONAMENTO else 'DESATIVADO'
    )

    print(
        'TOTAL DE REGISTROS PLANEJADOS:',
        f'{sum(metas.values()):,}'
    )

else:

    print(
        'MODO PLANEJAMENTO: nada gravado'
    )


# COMMAND ----------

# =====================================================
# VALIDACAO DE VOLUMETRIA E SCHEMA - V4
# =====================================================

# ORIGEM, DESTINO, MODO_AMOSTRA, LIMITE_AMOSTRA
# e ALVOS devem estar definidos na celula anterior.

BASES = list(ALVOS.keys())

aprovados = 0
falhas = []

for nome in BASES:

    original = spark.read.parquet(f"{ORIGEM}/{nome}")
    gerado = spark.read.parquet(f"{DESTINO}/{nome}")

    qtd_original = original.count()
    qtd_gerado = gerado.count()

    schema_ok = original.schema == gerado.schema

    if MODO_AMOSTRA:
        esperado = min(LIMITE_AMOSTRA, ALVOS[nome])
    else:
        esperado = ALVOS[nome]

    quantidade_ok = qtd_gerado == esperado

    status = "OK" if schema_ok and quantidade_ok else "FALHA"

    if status == "OK":
        aprovados += 1
    else:
        falhas.append(nome)

    print(
        f"[{status:5}] {nome:35} "
        f"original={qtd_original:>12,} "
        f"gerado={qtd_gerado:>12,} "
        f"esperado={esperado:>12,} "
        f"schema={'OK' if schema_ok else 'DIVERGENTE'}"
    )

print("\n========== RESUMO ==========")
print(f"Datasets aprovados: {aprovados}/{len(BASES)}")
print(f"Datasets com falha: {len(falhas)}")

if falhas:
    print("Falhas:", ", ".join(falhas))
else:
    print("VALIDACAO DE VOLUMETRIA E SCHEMA APROVADA")

# COMMAND ----------

# VALIDADOR V4.1 — Databricks Serverless / Spark Connect — SOMENTE LEITURA
# Compara problemas no DESTINO contra a ORIGEM usando diferenca multiconjunto (exceptAll).
# HERDADA = ocorrencia ja presente na origem; FALHA = ocorrencia nova no destino.
from pyspark.sql import functions as F

LIMITE_AMOSTRA = "{LIMITE_AMOSTRA}"
MOSTRAR_EXEMPLOS = 5
BASES = [
    'base_prices_current', 'products_discounts', 'selling_prices2_current',
    'discounts_v2', 'robinhood_campaigns', 'robinhood_campaign_products',
    'campaigns_v2', 'payment_methods', 'visao_catalogo_produto_atual',
    'prices', 'prices_historic', 'prices_penultimate_execution'
]

orig = {b: spark.read.parquet(f"{ORIGEM}/{b}") for b in BASES}
dest = {b: spark.read.parquet(f"{DESTINO}/{b}") for b in BASES}
resultados = []

def registrar(tipo, teste, herdadas=0, novas=0, detalhe='', status=None):
    status = status or ('FALHA' if novas else 'HERDADA' if herdadas else 'OK')
    resultados.append((tipo, teste, status, int(herdadas), int(novas), detalhe))
    print(f'[{status:13}] {tipo:12} {teste:58} herdadas={herdadas:,} novas={novas:,} {detalhe}')

def chaves(df, cols):
    return df.select(*[F.col(c).alias(f'__k{i}') for i, c in enumerate(cols)])

def verificar_relacao(filho, pai, cols_f, cols_p=None):
    cols_p = cols_p or cols_f
    nome = f'{filho} -> {pai}'
    ausentes = ([f'filho.{c}' for c in cols_f if c not in dest[filho].columns] +
                [f'pai.{c}' for c in cols_p if c not in dest[pai].columns])
    if ausentes:
        registrar('REFERENCIA', nome, detalhe=f'colunas ausentes: {ausentes}', status='NAO_APLICAVEL')
        return
    keys = [f'__k{i}' for i in range(len(cols_f))]
    def problemas(fonte):
        f = chaves(fonte[filho], cols_f)
        p = chaves(fonte[pai], cols_p).dropna(subset=keys).distinct()
        nulos = f.filter(' OR '.join(f'`{k}` IS NULL' for k in keys))
        orfaos = f.dropna(subset=keys).join(p, keys, 'left_anti')
        return nulos, orfaos
    (n0, o0), (n1, o1) = problemas(orig), problemas(dest)
    for tipo, antigo, atual in [('CHAVE_NULA', n0, n1), ('REFERENCIA', o0, o1)]:
        # Subtracao de multiconjuntos, nao apenas distinct: preserva multiplicidades.
        novas_df = atual.exceptAll(antigo)
        qtd_novas = novas_df.count()
        qtd_atual = atual.count()
        qtd_herdadas = qtd_atual - qtd_novas
        registrar(tipo, nome, qtd_herdadas, qtd_novas, f'total_destino={qtd_atual:,}')
        if qtd_novas and MOSTRAR_EXEMPLOS:
            print('  Exemplos de ocorrencias novas:')
            novas_df.show(MOSTRAR_EXEMPLOS, truncate=False)

def verificar_unicidade(base, coluna):
    nome = f'{base}: {coluna}'
    if coluna not in dest[base].columns:
        registrar('UNICIDADE', nome, status='NAO_APLICAVEL', detalhe='coluna ausente')
        return
    def repetidas(df):
        return df.groupBy(coluna).count().filter(F.col('count') > 1).select(coluna)
    antes = repetidas(orig[base])
    depois = repetidas(dest[base])
    novas = depois.join(antes, [coluna], 'left_anti')
    qtd_novas = novas.count()
    qtd_herdadas = depois.count() - qtd_novas
    registrar('UNICIDADE', nome, qtd_herdadas, qtd_novas,
              'conta chaves repetidas; repeticoes adicionais em chave herdada exigem exame separado')
    if qtd_novas and MOSTRAR_EXEMPLOS:
        novas.show(MOSTRAR_EXEMPLOS, truncate=False)
    # Testa se a quantidade de ocorrencias de uma chave cresceu alem das originais.
    c0 = orig[base].groupBy(coluna).count().withColumnRenamed('count', 'antes')
    c1 = dest[base].groupBy(coluna).count().withColumnRenamed('count', 'depois')
    cresceu = (c1.join(c0, [coluna], 'left')
                 .filter((F.col('antes') > 1) & (F.col('depois') > F.col('antes'))))
    qtd_cresceu = cresceu.count()
    registrar('UNICIDADE', f'{nome} (repeticoes ampliadas)', 0, qtd_cresceu,
              'chaves ja duplicadas na origem com mais ocorrencias no destino')

print('\n========== 1. CONTAGEM E SCHEMA ==========')
for b in BASES:
    n0, n1 = orig[b].count(), dest[b].count()
    esperado = ALVOS[b]
    registrar('CONTAGEM', b, 0, int(n1 != esperado), f'original={n0:,} destino={n1:,} esperado={esperado:,}')
    registrar('SCHEMA', b, 0, int(orig[b].schema != dest[b].schema))

print('\n========== 2. REFERENCIAS DE PRODUTO ==========')
for f in ['selling_prices2_current', 'prices', 'prices_penultimate_execution',
          'prices_historic', 'visao_catalogo_produto_atual']:
    verificar_relacao(f, 'base_prices_current', ['seller_id', 'sku'])

print('\n========== 3. CAMPANHAS E DESCONTOS ==========')
verificar_relacao('products_discounts', 'discounts_v2', ['campaign_id'], ['id'])
verificar_relacao('robinhood_campaign_products', 'robinhood_campaigns', ['campaign_id'])
verificar_relacao('robinhood_campaign_products', 'visao_catalogo_produto_atual', ['seller_id', 'sku'])
verificar_relacao('products_discounts', 'visao_catalogo_produto_atual', ['seller_id', 'sku'])

print('\n========== 4. UNICIDADE ==========')
verificar_unicidade('discounts_v2', 'id')
verificar_unicidade('robinhood_campaigns', 'campaign_id')

print('\n========== 5. RESUMO ==========')
resumo = spark.createDataFrame(resultados, 'categoria string, teste string, status string, herdadas long, novas long, detalhe string')
resumo.groupBy('status').count().orderBy('status').show(truncate=False)
print('TESTES COM FALHA:', sum(1 for r in resultados if r[2] == 'FALHA'))
resumo.filter(F.col('status') == 'FALHA').show(100, truncate=False)
print('OBS: referencias e unicidade sao hipoteses do modelo; validar com regras de negocio.')
print('OBS: exceptAll compara ocorrencias de chaves; nao certifica regras monetarias ou estruturas aninhadas.')


# COMMAND ----------

from pyspark.sql import functions as F

ROOT = "/Volumes/workspace/default/dados_teste_visao_unica"


def carregar(base, origem):
    return spark.read.parquet(f"{origem}/{base}")

def verificar_orfaos(base_filha, base_pai, chaves_filha, chaves_pai, origem):
    filho = carregar(base_filha, origem)
    pai = carregar(base_pai, origem)

    f = filho.select(
        *[F.col(c).alias(f"k{i}") for i, c in enumerate(chaves_filha)]
    )

    p = pai.select(
        *[F.col(c).alias(f"k{i}") for i, c in enumerate(chaves_pai)]
    ).distinct()

    return f.join(p, [f"k{i}" for i in range(len(chaves_filha))], "left_anti").count()

relacoes = [
    ("robinhood_campaign_products", "robinhood_campaigns",
     ["campaign_id"], ["campaign_id"]),

    ("robinhood_campaign_products", "visao_catalogo_produto_atual",
     ["seller_id", "sku"], ["seller_id", "sku"])
]

print("========== ORIGINAIS X GERADOS ==========")

for filho, pai, chaves_filho, chaves_pai in relacoes:
    print(f"\n{filho} -> {pai}")

    for nome, caminho in [
        ("ORIGINAL", ORIGEM),
        ("GERADO", DESTINO)
    ]:
        qtd = verificar_orfaos(
            filho, pai, chaves_filho, chaves_pai, caminho
        )

        print(f"{nome:10} órfãos={qtd}")

print("\n========== DUPLICIDADES ==========")

for base, chave in [
    ("discounts_v2", "id"),
    ("robinhood_campaigns", "campaign_id")
]:
    print(f"\n{base} / {chave}")

    for nome, caminho in [
        ("ORIGINAL", ORIGEM),
        ("GERADO", DESTINO)
    ]:
        df = carregar(base, caminho)

        duplicados = (
            df.groupBy(chave)
              .count()
              .filter(F.col("count") > 1)
              .count()
        )

        print(f"{nome:10} chaves duplicadas={duplicados}")