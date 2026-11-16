# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
ORIGEM = '/Volumes/test/default/dados_origin_visao_unica/parquet'
DESTINO = "/Volumes/test/default/dados_origin_visao_unica/parquet_volumetria_final"


EXECUTAR_GRAVACAO = True
MODO_AMOSTRA = False
LIMITE_AMOSTRA = 628890840

# Volumetria Alvo para cada base
ALVOS = {
    "selling_prices2_current": 901802225,
    "base_prices_current": 776858317,
    "prices": 15983801,
    "prices_penultimate_execution": 164465199,
    "robinhood_campaign_products": 73199718,
    "campaigns_v2": 35210395,
    "visao_catalogo_produto_atual": 32201680,
    "prices_historic": 18374983,
    "robinhood_campaigns": 1429841,
    "discounts_v2": 572498,
    "products_discounts": 91253,
    "payment_methods": 36
}

print("ORIGEM:", ORIGEM)
print("DESTINO:", DESTINO)

# COMMAND ----------

CALIBRACAO_ATIVA = True

COLUNAS_TEXTO_CALIBRACAO = {
    "campaigns_v2": "description",
    "visao_catalogo_produto_atual": "description",
    "robinhood_campaigns": "description",
    "discounts_v2": "description"
}

FATOR_TEXTO = 1.3
MAX_CARACTERES_CALIBRACAO = 512

# COMMAND ----------

# Databricks Serverless - Gerador V13 (Snappy seletivo, sem amostragem)
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

# SEGURANCA: permanece False ate autorizacao explicita.
EXECUTAR_GRAVACAO = True

# Somente cinco datasets terao dictionary encoding desativado; codec Snappy em todos.
DATASETS_AJUSTE_FISICO = {
    "selling_prices2_current", "base_prices_current", "prices",
    "prices_penultimate_execution", "visao_catalogo_produto_atual"
}
# Nao altera chaves nem adiciona colunas; evita modificar regras do pipeline.
CALIBRACAO_ATIVA = False


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
print('CONFIGURACAO GERADOR V13')
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
print('PLANEJAMENTO V13')

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

# Limita cardinalidade: valores repetidos comprimem melhor.
# Ajustavel na celula de configuracao, sem alterar ALVOS.
# Cardinalidade padrao; pode ser definida na celula anterior.
CARDINALIDADE_SINTETICA = globals().get('CARDINALIDADE_SINTETICA', 14_000_000)

CARDINALIDADE_POR_DATASET = {
    'selling_prices2_current': 15_000_000,
    'base_prices_current': 54_000_000,
    'prices': 136_000_000,
    'prices_penultimate_execution': 48_000_000,
    'robinhood_campaign_products': 1200_000,
    'campaigns_v2': 51_400_000,
    'visao_catalogo_produto_atual': 520_000_000,
    'prices_historic': 300_000,
    'robinhood_campaigns': 1600_000,
    'discounts_v2': 1400_000,
    'products_discounts': 10_000,
    'payment_methods': 36,
}
CARDINALIDADE_ATUAL = CARDINALIDADE_SINTETICA
if type(CARDINALIDADE_SINTETICA) is not int or CARDINALIDADE_SINTETICA < 1:
    raise ValueError('CARDINALIDADE_SINTETICA deve ser inteiro positivo')

def id_logico(nome, indice):
    return F.concat(
        F.lit('SYN_' + nome + '_'),
        F.lpad(
            F.pmod(indice.cast('long'), F.lit(CARDINALIDADE_ATUAL))
             .cast('string'), 4, '0'
        )
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
# 6A. CALIBRACAO DE TAMANHO - SOMENTE COLUNAS AUTORIZADAS
# ============================================================
# IMPORTANTE: a meta de bytes NAO pode ser garantida por escrita Parquet.
# Preencha as colunas APOS conferir os schemas e as regras do pipeline.
# Somente colunas STRING, nao usadas em JOIN/filtro/regra de negocio.
# Sem colunas autorizadas, nenhuma alteracao de conteudo e aplicada.
# Nunca adiciona coluna ao schema final.

CALIBRACAO_ATIVA = False
COLUNAS_TEXTO_CALIBRACAO = globals().get('COLUNAS_TEXTO_CALIBRACAO', {
    # Exemplo APENAS se existir e for comprovadamente nao critica:
    # 'selling_prices2_current': 'description',
})
# Tamanhos observados na ultima validacao (bytes decimais).
# Ajuste estes valores ao executar novas rodadas.
TAMANHOS_BASE_BYTES = {
    'selling_prices2_current': 8.71e9,
    'base_prices_current': 2.21e9,
    'prices': 176.97e6,
    'prices_penultimate_execution': 1.54e9,
    'robinhood_campaign_products': 508.46e6,
    'campaigns_v2': 389.97e6,
    'visao_catalogo_produto_atual': 152.40e6,
    'prices_historic': 188.47e6,
    'robinhood_campaigns': 19.73e6,
    'discounts_v2': 20.70e6,
    'products_discounts': 1.25e6,
    'payment_methods': 9.49e3,
}
# Estimativa conservadora de bytes adicionais de texto por registro.
# Ajustar com base na medicao, pois Snappy/dicionario afetam o resultado.
FATOR_TEXTO = globals().get('FATOR_TEXTO', 1.3)
MAX_CARACTERES_CALIBRACAO = globals().get('MAX_CARACTERES_CALIBRACAO', 512)


def verificar_colunas_calibracao():
    for nome, coluna in COLUNAS_TEXTO_CALIBRACAO.items():
        if nome not in originais:
            raise ValueError(f'Dataset desconhecido para calibracao: {nome}')
        if coluna not in originais[nome].columns:
            raise ValueError(f'{nome}: coluna inexistente: {coluna}')
        if not isinstance(originais[nome].schema[coluna].dataType, T.StringType):
            raise TypeError(f'{nome}.{coluna}: calibracao exige STRING')
        bloqueadas = ('id', 'sku', 'seller', 'key', 'code', 'date', 'time', 'status',
                      'price', 'category', 'brand', 'channel', 'currency', 'op',
                      'action', 'type', 'benefit', 'method')
        if any(token in coluna.lower() for token in bloqueadas):
            raise ValueError(f'{nome}.{coluna}: nome potencialmente critico; nao calibrar')


def calibrar_texto(df, nome):
    if not CALIBRACAO_ATIVA or nome not in COLUNAS_TEXTO_CALIBRACAO:
        return df
    coluna = COLUNAS_TEXTO_CALIBRACAO[nome]
    faltantes = max(0.0, TAMANHO_REAL_BYTES[nome] - TAMANHOS_BASE_BYTES[nome])
    if faltantes <= 0:
        print(f'[CALIBRACAO] {nome}: acima da referencia, sem expansao')
        return df
    # __seq existe somente no DataFrame sintetico.
    caracteres = min(MAX_CARACTERES_CALIBRACAO,
                     max(1, math.ceil(faltantes / max(1, qtd_syn[nome]) * FATOR_TEXTO)))
    # Hash SHA-256 hexadecimal variavel por linha, repetido apenas se necessario.
    # O tamanho real comprimido sera medido pelo validador.
    hash_ = F.sha2(F.concat(F.lit(nome), F.col('__seq').cast('string')), 256)
    texto = F.substring(F.repeat(hash_, math.ceil(caracteres / 64)), 1, caracteres)
    print(f'[CALIBRACAO] {nome}.{coluna}: ate {caracteres} caracteres sinteticos/linha')
    return df.withColumn(coluna, texto)


verificar_colunas_calibracao()
if not COLUNAS_TEXTO_CALIBRACAO:
    print('[CALIBRACAO] Nenhuma coluna autorizada: geracao permanece inalterada.')
    print('[COLUNAS CANDIDATAS - SOMENTE INSPECAO]')
    for n, d in originais.items():
        cols = [f.name for f in d.schema.fields if isinstance(f.dataType, T.StringType)]
        print(f'  {n}: {cols}')


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

    # Ciclo curto de valores para favorecer dictionary encoding.
    idx = F.pmod(F.col('__seq'), F.lit(CARDINALIDADE_ATUAL))

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

    df = calibrar_texto(df, nome)
    return df.select(
        *base.columns
    )


# ============================================================
# 8. GRAVACAO - REFERENCIA FISICA E COMPRESSAO DOS DADOS REAIS
# ============================================================
# Quantidades de LINHAS continuam vindo SOMENTE de ALVOS.
# ATENCAO: chaves repetidas podem alterar JOINs, DISTINCT e skew.
# Para teste de volumetria, nao para equivalencia funcional.
# Os valores abaixo controlam exclusivamente o layout fisico.

ARQUIVOS_REFERENCIA = {
    'selling_prices2_current': 3044,
    'base_prices_current': 741,
    'prices': 79,
    'prices_penultimate_execution': 79,
    'robinhood_campaign_products': 867,
    'campaigns_v2': 393,
    'visao_catalogo_produto_atual': 564,
    'prices_historic': 11,
    'robinhood_campaigns': 1730,
    'discounts_v2': 937,
    'products_discounts': 4,
    'payment_methods': 2,
}

# Valores informativos para a validacao posterior; nao sao metas
# de escrita em bytes, pois compressibilidade depende dos dados.
TAMANHO_REAL_BYTES = {
    'selling_prices2_current': 19.7 * 10**9,
    'base_prices_current': 14.1 * 10**9,
    'prices': 6.1 * 10**9,
    'prices_penultimate_execution': 6.1 * 10**9,
    'robinhood_campaign_products': 744.5 * 10**6,
    'campaigns_v2': 1.3 * 10**9,
    'visao_catalogo_produto_atual': 8.8 * 10**9,
    'prices_historic': 65.9 * 10**6,
    'robinhood_campaigns': 96.0 * 10**6,
    'discounts_v2': 85.6 * 10**6,
    'products_discounts': 0.6 * 10**6,
    'payment_methods': 9.6 * 10**3,
}

# Coluna fisica de particionamento, quando identificavel no schema.
# Nunca cria uma coluna nova e nao altera ALVOS.
CANDIDATAS_DATA = {
    'robinhood_campaign_products': ['timestamp_kafka_day', 'price_date'],
    'campaigns_v2': ['timestamp_kafka_day', 'price_date'],
    'prices_historic': ['price_date', 'timestamp_kafka_day'],
    'robinhood_campaigns': ['timestamp_kafka_day', 'price_date'],
    'discounts_v2': ['timestamp_kafka_day', 'price_date'],
    'products_discounts': ['timestamp_kafka_day', 'price_date'],
}

# A imagem de referencia mostra 41, 22, 1, 43, 43 e 4 dias,
# respectivamente. Este script preserva as datas efetivamente
# presentes no DataFrame; nao inventa nem altera distribuicoes.
DIAS_REFERENCIA = {
    'robinhood_campaign_products': 41,
    'campaigns_v2': 22,
    'prices_historic': 1,
    'robinhood_campaigns': 43,
    'discounts_v2': 43,
    'products_discounts': 4,
}

# Evita que configuracoes de outras celulas desabilitem o layout
# de referencia na gravacao. PARTICIONAMENTO continua sendo usado
# pela funcao de geracao de registros sinteticos (secao 7).


def arquivos_parquet_recursivo(path):
    encontrados = []
    for item in dbutils.fs.ls(path):
        if item.isDir():
            encontrados.extend(arquivos_parquet_recursivo(item.path))
        elif item.path.lower().endswith('.parquet'):
            encontrados.append(item)
    return encontrados


def escolher_coluna_data(nome, dados):
    for coluna in CANDIDATAS_DATA.get(nome, []):
        if coluna in dados.columns:
            return coluna
    return None


if EXECUTAR_GRAVACAO and CALIBRACAO_ATIVA and not COLUNAS_TEXTO_CALIBRACAO:
    raise ValueError('Defina COLUNAS_TEXTO_CALIBRACAO antes de executar a gravacao V13; veja candidatas acima.')

if EXECUTAR_GRAVACAO:
    # Protecao original: nao sobrescrever um destino ja existente.
    try:
        dbutils.fs.ls(DESTINO)
    except Exception as exc:
        if not any(t in str(exc).lower() for t in (
            'not found', 'does not exist', 'filenotfound', 'path_not_found'
        )):
            raise
    else:
        raise RuntimeError(
            f'DESTINO JA EXISTE: {DESTINO}; escolha outro destino'
        )

    for nome in ALVOS:
        CARDINALIDADE_ATUAL = CARDINALIDADE_POR_DATASET.get(nome, CARDINALIDADE_SINTETICA)
        print(f'\n[GERANDO] {nome} | meta linhas={metas[nome]:,}')
        dados = (
            originais[nome] if qtd_syn[nome] == 0
            else originais[nome].unionByName(sinteticos(nome))
        )

        alvo_arquivos = ARQUIVOS_REFERENCIA.get(nome)
        if alvo_arquivos is None:
            raise ValueError(f'Sem referencia de arquivos: {nome}')

        # Em modo amostra, evita criar milhares de arquivos vazios.
        if MODO_AMOSTRA:
            alvo_arquivos = min(alvo_arquivos, max(1, metas[nome]))

        caminho = f'{DESTINO.rstrip("/")}/{nome}'

        # Sem partitionBy: nenhuma pasta fisica por data.
        # Spark continua usando particoes internas para escrever.
        CODEC = "snappy"
        DICIONARIO = "false" if nome in DATASETS_AJUSTE_FISICO else "true"
        dados = dados.repartition(alvo_arquivos)
        print(f'[SEM PARTITIONBY] tarefas de escrita={alvo_arquivos}')
        print(f'[CARDINALIDADE] {CARDINALIDADE_ATUAL:,} | codec={CODEC} | dictionary={DICIONARIO}')

        # Snappy para todos; desabilita dictionary encoding apenas nos cinco alvos.
        # O tamanho final depende dos dados e nao e garantido.
        (dados.write.mode("overwrite")
            .option("compression", CODEC)
            .option("parquet.enable.dictionary", DICIONARIO)
            .parquet(caminho))

        arquivos = arquivos_parquet_recursivo(caminho)
        tamanho = sum(a.size for a in arquivos)
        real = TAMANHO_REAL_BYTES[nome]
        diferenca = (tamanho / real - 1) * 100
        print(
            f'[GRAVADO] {nome} | arquivos={len(arquivos):,} '
            f'(referencia={ARQUIVOS_REFERENCIA[nome]:,}) | '
            f'tamanho={tamanho / 10**9:.3f} GB | '
            f'diferenca tamanho={diferenca:+.1f}%'
        )
        if len(arquivos) != ARQUIVOS_REFERENCIA[nome] and not MODO_AMOSTRA:
            print('[AVISO] Quantidade fisica de arquivos difere da referencia.')
        if nome in DIAS_REFERENCIA:
            print(
                '[REFERENCIA DE DIAS]', DIAS_REFERENCIA[nome],
                '| verificar valores distintos da coluna no validador'
            )

    print('\nPROCESSAMENTO FINALIZADO | DESTINO:', DESTINO)
    print('TOTAL DE REGISTROS PLANEJADOS:', f'{sum(metas.values()):,}')
else:
    print('\n' + '=' * 112)
    print('PLANEJAMENTO DE GRAVACAO V13 | NENHUM ARQUIVO SERA CRIADO OU ALTERADO')
    print('=' * 112)

    # Mede os arquivos existentes, quando disponiveis. A medicao e real;
    # a previsao de uma NOVA gravacao e apenas uma referencia aproximada.
    # O conteudo, a cardinalidade e o codec podem alterar drasticamente os bytes.
    def medir_parquets_existentes(path):
        try:
            arquivos = arquivos_parquet_recursivo(path)
            return len(arquivos), sum(a.size for a in arquivos)
        except Exception:
            return None, None

    def tamanho_legivel(valor):
        if valor is None:
            return 'N/D'
        if valor >= 1e9:
            return f'{valor / 1e9:,.2f} GB'
        if valor >= 1e6:
            return f'{valor / 1e6:,.2f} MB'
        return f'{valor / 1e3:,.2f} KB'

    linhas_total = 0
    bytes_ref_total = 0
    bytes_existentes_total = 0
    encontrados = 0
    previsao = []

    for nome in ALVOS:
        linhas = metas[nome]
        arquivos_meta = ARQUIVOS_REFERENCIA[nome]
        bytes_ref = TAMANHO_REAL_BYTES[nome]
        caminho_existente = f'{DESTINO.rstrip("/")}/{nome}'
        arq_existentes, bytes_existentes = medir_parquets_existentes(caminho_existente)
        # Se a saida ja existe, seu tamanho e uma observacao objetiva,
        # NAO uma estimativa confiavel da nova geracao.
        # Se nao existe, usa a ultima medida V10 apenas como referencia historica.
        base_bytes = bytes_existentes if bytes_existentes is not None else TAMANHOS_BASE_BYTES.get(nome)
        fonte = 'MEDIDO NO DESTINO' if bytes_existentes is not None else 'HISTORICO V10'
        if bytes_existentes is not None:
            encontrados += 1
            bytes_existentes_total += bytes_existentes
        linhas_total += linhas
        bytes_ref_total += bytes_ref
        previsao.append((nome, linhas, arquivos_meta, bytes_ref, base_bytes, fonte))

    print(f'{"DATASET":35} {"LINHAS":>14} {"ARQUIVOS":>8} {"META REAL":>13} {"BASE OBSERVADA":>16}  FONTE')
    print('-' * 112)
    for nome, linhas, arqs, ref, base, fonte in previsao:
        print(f'{nome:35} {linhas:>14,} {arqs:>8,} {tamanho_legivel(ref):>13} {tamanho_legivel(base):>16}  {fonte}')
    print('-' * 112)
    print(f'TOTAL DE LINHAS PREVISTAS: {linhas_total:,}')
    print(f'TOTAL DE ARQUIVOS-ALVO: {sum(ARQUIVOS_REFERENCIA[n] for n in ALVOS):,}')
    print(f'TAMANHO TOTAL DE REFERENCIA REAL: {tamanho_legivel(bytes_ref_total)}')
    print(f'DATASETS COM TAMANHO MEDIDO NO DESTINO: {encontrados}/{len(ALVOS)}')
    print(f'SOMA DOS TAMANHOS MEDIDOS NO DESTINO: {tamanho_legivel(bytes_existentes_total)}')
    print('\nAVISO: BASE OBSERVADA NAO E PREVISAO CALIBRADA DA PROXIMA GRAVACAO.')
    print('Cinco datasets usarao SNAPPY e dictionary=false.')
    print('Os demais usarao SNAPPY e dictionary=true.')
    print('Tamanhos finais NAO sao previsiveis com precisao sem gravacao.')
    print('MODO PLANEJAMENTO: EXECUTAR_GRAVACAO=False; nenhum dado gravado.')


# COMMAND ----------

# ============================================================
# VALIDADOR V6 - VOLUMETRIA FISICA DOS DATASETS
# ============================================================

# Referencia: imagem dos dados reais
# Formato: (linhas, arquivos, tamanho, unidade)

REFERENCIA = {
    "selling_prices2_current": (901802225, 3044, 19.7, "GB"),
    "base_prices_current": (776858317, 741, 14.1, "GB"),
    "prices": (164465199, 79, 6.1, "GB"),
    "prices_penultimate_execution": (164465199, 79, 6.1, "GB"),
    "robinhood_campaign_products": (73199718, 867, 744.5, "MB"),
    "campaigns_v2": (35210395, 393, 1.3, "GB"),
    "visao_catalogo_produto_atual": (32201680, 564, 8.8, "GB"),
    "prices_historic": (2391182, 11, 65.9, "MB"),
    "robinhood_campaigns": (1429841, 1730, 96.0, "MB"),
    "discounts_v2": (572498, 937, 85.6, "MB"),
    "products_discounts": (91253, 4, 0.6, "MB"),
    "payment_methods": (36, 2, 9.6, "KB")
}

TOLERANCIA_TAMANHO_PCT = 5.0

def tamanho_bytes(valor, unidade):
    fatores = {
        "GB": 1000**3,
        "MB": 1000**2,
        "KB": 1000
    }
    return valor * fatores[unidade]


def listar_parquets(caminho):
    arquivos = []
    pilha = [caminho]

    while pilha:
        atual = pilha.pop()

        for item in dbutils.fs.ls(atual):
            if item.isDir():
                pilha.append(item.path)
            elif item.path.lower().endswith(".parquet"):
                arquivos.append(item)

    return arquivos


def formatar_tamanho(bytes_total):
    if bytes_total >= 1000**3:
        return f"{bytes_total / 1000**3:.2f} GB"
    if bytes_total >= 1000**2:
        return f"{bytes_total / 1000**2:.2f} MB"
    return f"{bytes_total / 1000:.2f} KB"


resultados = []

for nome, (linhas_ref, arquivos_ref, valor_ref, unidade) in REFERENCIA.items():

    print(f"[VALIDANDO] {nome}")

    try:
        caminho = f"{DESTINO.rstrip('/')}/{nome}"

        # Quantidade real de registros
        df = spark.read.parquet(caminho)
        linhas_geradas = df.count()

        # Arquivos fisicos
        arquivos = listar_parquets(caminho)
        arquivos_gerados = len(arquivos)

        # Tamanho fisico dos Parquets
        bytes_gerados = sum(a.size for a in arquivos)
        bytes_referencia = tamanho_bytes(valor_ref, unidade)

        # Diferencas
        diferenca_linhas = linhas_geradas - linhas_ref
        diferenca_arquivos = arquivos_gerados - arquivos_ref

        diferenca_tamanho_pct = (
            (bytes_gerados - bytes_referencia)
            / bytes_referencia * 100
        )

        # Status individuais
        # Em modo amostra, respeita a meta da celula anterior
        esperado_linhas = (
            min(LIMITE_AMOSTRA, ALVOS[nome])
            if MODO_AMOSTRA
            else linhas_ref
        )

        linhas_ok = linhas_geradas == esperado_linhas

        arquivos_ok = (
            True if MODO_AMOSTRA
            else arquivos_gerados == arquivos_ref
        )

        tamanho_ok = (
            True if MODO_AMOSTRA
            else abs(diferenca_tamanho_pct) <= TOLERANCIA_TAMANHO_PCT
        )

        status = (
            "OK"
            if linhas_ok and arquivos_ok and tamanho_ok
            else "FALHA"
        )

        resultados.append({
            "Dataset": nome,
            "Status": status,
            "Linhas esperadas": esperado_linhas,
            "Linhas geradas": linhas_geradas,
            "Linhas OK": linhas_ok,
            "Arquivos reais": arquivos_ref,
            "Arquivos gerados": arquivos_gerados,
            "Arquivos OK": arquivos_ok,
            "Tamanho real": f"{valor_ref} {unidade}",
            "Tamanho gerado": formatar_tamanho(bytes_gerados),
            "Diferença tamanho %": round(diferenca_tamanho_pct, 2),
            "Tamanho OK": tamanho_ok
        })

    except Exception as erro:
        resultados.append({
            "Dataset": nome,
            "Status": "ERRO",
            "Erro": str(erro)
        })
        print(f"[ERRO] {nome}: {erro}")


# ============================================================
# RESULTADO FINAL
# ============================================================

from pyspark.sql import types as T

schema_resultado = T.StructType([
    T.StructField("Dataset", T.StringType()),
    T.StructField("Status", T.StringType()),
    T.StructField("Linhas esperadas", T.LongType()),
    T.StructField("Linhas geradas", T.LongType()),
    T.StructField("Linhas OK", T.BooleanType()),
    T.StructField("Arquivos reais", T.IntegerType()),
    T.StructField("Arquivos gerados", T.IntegerType()),
    T.StructField("Arquivos OK", T.BooleanType()),
    T.StructField("Tamanho real", T.StringType()),
    T.StructField("Tamanho gerado", T.StringType()),
    T.StructField("Diferença tamanho %", T.DoubleType()),
    T.StructField("Tamanho OK", T.BooleanType()),
    T.StructField("Erro", T.StringType())
])

# Normaliza os resultados para exibição
dados_resultado = [
    {campo.name: r.get(campo.name) for campo in schema_resultado}
    for r in resultados
]

df_resultado = spark.createDataFrame(
    dados_resultado,
    schema=schema_resultado
)

display(df_resultado)

aprovados = sum(r["Status"] == "OK" for r in resultados)
falhas = len(resultados) - aprovados

print("\n" + "=" * 65)
print("RESUMO DA VALIDACAO V6")
print("=" * 65)
print(f"Datasets avaliados: {len(resultados)}")
print(f"Aprovados: {aprovados}")
print(f"Falhas: {falhas}")
print("=" * 65)

# COMMAND ----------

# INVENTARIO DE DATASETS - VISAO UNICA DE PRECOS
# ============================================================

import os
from datetime import datetime
from pyspark.sql import Row

# ============================================================
# 1. CONFIGURACOES
# ============================================================

BASE = "/Volumes/test/default/dados_origin_visao_unica"

DIRETORIOS = {
    "DADOS SINTÉTICOS": f"{BASE}/parquet_volumetria_final",
    "RESULTADOS": f"{BASE}/resultados"
}

# ============================================================
# 2. FUNCOES AUXILIARES
# ============================================================

def formatar_numero(valor):
    return f"{valor:,}".replace(",", ".")


def formatar_tamanho(bytes_total):

    if bytes_total >= 1024 ** 3:
        return f"{bytes_total / (1024 ** 3):,.1f} GB".replace(",", "X").replace(".", ",").replace("X", ".")

    if bytes_total >= 1024 ** 2:
        return f"{bytes_total / (1024 ** 2):,.1f} MB".replace(",", "X").replace(".", ",").replace("X", ".")

    return f"{bytes_total / 1024:,.1f} KB".replace(",", "X").replace(".", ",").replace("X", ".")


def listar_arquivos(caminho):

    arquivos = []

    for raiz, diretorios, nomes in os.walk(caminho):

        for nome in nomes:

            if nome.lower().endswith(".parquet"):

                arquivo = os.path.join(raiz, nome)

                arquivos.append(arquivo)

    return arquivos


def identificar_particoes(caminho):

    particoes = set()

    for raiz, diretorios, arquivos in os.walk(caminho):

        relativo = os.path.relpath(raiz, caminho)

        if relativo == ".":
            continue

        partes = relativo.split(os.sep)

        for parte in partes:

            if "=" in parte:
                particoes.add(parte)

    if not particoes:
        return "nenhuma"

    return f"{len(particoes)} partições"


# ============================================================
# 3. INVENTARIO POR DIRETORIO
# ============================================================

for tipo, diretorio in DIRETORIOS.items():

    print()
    print("=" * 100)
    print(f" DATASETS - {tipo}")
    print("=" * 100)

    if not os.path.isdir(diretorio):

        print(f"[AVISO] Diretório não encontrado: {diretorio}")
        continue

    registros = []

    datasets = sorted(os.listdir(diretorio))

    for dataset in datasets:

        caminho = os.path.join(diretorio, dataset)

        if not os.path.isdir(caminho):
            continue

        try:

            # Quantidade real de registros
            linhas = spark.read.parquet(caminho).count()

            # Arquivos Parquet
            arquivos = listar_arquivos(caminho)

            quantidade_arquivos = len(arquivos)

            # Tamanho físico
            tamanho_bytes = sum(
                os.path.getsize(arquivo)
                for arquivo in arquivos
            )

            # Partições
            particoes = identificar_particoes(caminho)

            registros.append({
                "Dataset": dataset,
                "Linhas": formatar_numero(linhas),
                "Arquivos": formatar_numero(quantidade_arquivos),
                "Tamanho": formatar_tamanho(tamanho_bytes),
                "Partições": particoes,
                "Mudança última revisão": "Não avaliado",
                "_ordem": linhas
            })

            print(f"[OK] {dataset}", flush=True)

        except Exception as erro:

            print(f"[ERRO] {dataset}: {str(erro)[:180]}")

    # ========================================================
    # 4. EXIBIR TABELA
    # ========================================================

    if registros:

        registros = sorted(
            registros,
            key=lambda x: x["_ordem"],
            reverse=True
        )

        for item in registros:
            del item["_ordem"]

        display(
            spark.createDataFrame(registros)
            .select(
                "Dataset",
                "Linhas",
                "Arquivos",
                "Tamanho",
                "Partições",
                "Mudança última revisão"
            )
        )

    else:
        print("Nenhum dataset encontrado.")
