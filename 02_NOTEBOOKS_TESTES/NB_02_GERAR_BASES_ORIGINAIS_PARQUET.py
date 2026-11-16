# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import os
# Caminho do Volume no Databricks
CAMINHO_VOLUME = (
    "/Volumes/test/default/"
    "dados_origin_visao_unica"
)

# Caminho do arquivo ZIP
ARQUIVO_ZIP = f"{CAMINHO_VOLUME}/Bases_Visao_unica.zip"

# Caminho do arquivo PARQUET
DESTINO_PARQUET = '/Volumes/test/default/dados_origin_visao_unica/parquet'

# Caminho do Relatório
DESTINO_RELATORIO='/Volumes/test/default/dados_origin_visao_unica/relatorios_lote11_originais'

print("Volume:", CAMINHO_VOLUME)
print("Arquivo ZIP:", ARQUIVO_ZIP)

# Verificar se o arquivo existe
print("ZIP encontrado:", os.path.isfile(ARQUIVO_ZIP))

# COMMAND ----------

# Databricks - executar como notebook Python em UMA celula.
# 12 CSVs originais, SEM multiplicacao e SEM correcao de IDs.
# Executar como notebook Python em UMA célula.
# 12 CSVs originais, SEM multiplicacao e SEM correcao de IDs.

import ast
import csv
import io
import json
import re
import zipfile

from datetime import datetime, date, timezone
from pyspark.sql import types as T


# ============================================================
# 1. CONFIGURACOES
# ============================================================

ZIP = f'{ARQUIVO_ZIP}'
DESTINO = f'{DESTINO_PARQUET}'

SOBRESCREVER = True

# Controle opcional de reparticionamento
# False = nao aplica repartition explicitamente
# True  = aplica repartition(NUM_PARTICOES)
PARTICIONAMENTO = False

# Utilizado somente quando PARTICIONAMENTO = True
NUM_PARTICOES = 8

if not isinstance(PARTICIONAMENTO, bool):
    raise TypeError('PARTICIONAMENTO deve ser True ou False')

if PARTICIONAMENTO:
    if (
        isinstance(NUM_PARTICOES, bool)
        or not isinstance(NUM_PARTICOES, int)
        or NUM_PARTICOES < 1
    ):
        raise ValueError(
            'NUM_PARTICOES deve ser um inteiro maior que zero'
        )


# ============================================================
# 2. QUANTIDADES ESPERADAS
# ============================================================

ESPERADO = {
    'base_prices_current': 5,
    'campaigns_v2': 50,
    'discounts_v2': 24,
    'payment_methods': 18,
    'prices': 5,
    'prices_historic': 56,
    'prices_penultimate_execution': 5,
    'products_discounts': 39,
    'robinhood_campaign_products': 22,
    'robinhood_campaigns': 13,
    'selling_prices2_current': 5,
    'visao_catalogo_produto_atual': 10,
}


# ============================================================
# 3. CONTRATOS E TIPOS DE DADOS
# ============================================================

# Contrato conhecido pelo uso real no legado:
# array_intersect(
#     prices.selections,
#     discounts.target.exclude.selections
# )
#
# Nao inferir string para coluna totalmente nula
# quando o codigo exige array.

OVERRIDES = {
    ('base_prices_current', 'selections'):
        T.ArrayType(T.StringType(), True)
}

BOOL = {
    'active',
    'is_deleted',
    'campanha_expirada',
    'has_stock_control',
    'sells_to_company',
    'kit',
    'used',
    'stock'
}

INTEIROS = {
    'max_installment_without_interest',
    'max_installment_interestfree_luiza',
    'max_installment_interestfree_visa',
    'max_installment_luiza',
    'max_installment_visa'
}

NUMERICOS = {
    'list_price',
    'price',
    'min_installment_amount',
    'review_score',
    'depth',
    'width',
    'height',
    'weight',
    'price_de',
    'price_por',
    'price_pix',
    'price_boleto',
    'price_magalupay',
    'price_por_ouro',
    'price_pix_ouro',
    'installment_price_once_visa',
    'installment_price_once_luiza',
    'discount_perc',
    'discount_perc_pix',
    'discount_perc_ouro'
}

TIMESTAMPS = {
    'updated_at',
    'created_at',
    'deleted_at',
    'start_date',
    'end_date',
    'timestamp_kafka',
    'checkpoint_timestamp',
    'niagara_timestamp',
    'updated_timestamp',
    'last_updated_products',
    'campaign_end_date',
    'expiration_notified_at',
    'notified_at',
    'start_at',
    'end_at',
    'price_timestamp'
}

DATES = {
    'timestamp_kafka_day',
    'price_date'
}


# ============================================================
# 4. INTERPRETACAO DOS VALORES DOS CSVs
# ============================================================

def interpretar(texto):

    if texto is None:
        return None

    if not isinstance(texto, str):
        return texto

    s = texto.strip()

    if not s or s.lower() in ('null', 'none'):
        return None

    if re.search(r'__SIM\d+$', s):
        raise ValueError(
            'Marcador de expansao detectado em CSV ORIGINAL: '
            + repr(s[:90])
        )

    if s[0] not in '[{(' and not s.startswith('Row('):
        return s

    # JSON nativo ou Python repr/Row(...); sem exec/eval.
    try:
        return json.loads(s)
    except (ValueError, TypeError):
        pass

    s = re.sub(
        r'(?<![\w$])\$oid(?=\s*=)',
        '__DOLLAR_OID__',
        s
    )

    def node(n):

        if isinstance(n, ast.Constant):
            return n.value

        if (
            isinstance(n, ast.Name)
            and n.id in (
                'true', 'false', 'null',
                'True', 'False', 'None'
            )
        ):
            return {
                'true': True,
                'false': False,
                'null': None,
                'True': True,
                'False': False,
                'None': None
            }[n.id]

        if isinstance(n, (ast.List, ast.Tuple)):
            return [node(x) for x in n.elts]

        if isinstance(n, ast.Dict):
            return {
                node(k): node(v)
                for k, v in zip(n.keys, n.values)
            }

        if (
            isinstance(n, ast.UnaryOp)
            and isinstance(n.op, (ast.USub, ast.UAdd))
        ):
            v = node(n.operand)

            if (
                isinstance(v, bool)
                or not isinstance(v, (int, float))
            ):
                raise ValueError('Numero invalido')

            return -v if isinstance(n.op, ast.USub) else v

        if (
            isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name)
            and n.func.id == 'Row'
            and not n.args
        ):
            return {
                (
                    '$oid'
                    if k.arg == '__DOLLAR_OID__'
                    else k.arg
                ): node(k.value)
                for k in n.keywords
            }

        raise ValueError(
            'Expressao nao suportada: '
            + type(n).__name__
        )

    return node(ast.parse(s, mode='eval').body)


# ============================================================
# 5. CONVERSAO DE TIPOS
# ============================================================

def converter(coluna, v):

    if v is None or isinstance(v, (dict, list)):
        return v

    if coluna in BOOL:

        if isinstance(v, bool):
            return v

        if str(v).lower() not in ('true', 'false'):
            raise ValueError(
                'Booleano invalido ' + repr(v)
            )

        return str(v).lower() == 'true'

    if coluna in INTEIROS:
        return int(v)

    if coluna in NUMERICOS:
        return float(v)

    if coluna in DATES:
        return date.fromisoformat(str(v))

    if coluna in TIMESTAMPS:

        dt = datetime.fromisoformat(
            str(v).replace('Z', '+00:00')
        )

        return (
            dt.astimezone(timezone.utc).replace(tzinfo=None)
            if dt.tzinfo
            else dt
        )

    return v


# ============================================================
# 6. INFERENCIA DE TIPOS PYSPARK
# ============================================================

def tipo(v):

    if v is None:
        return T.NullType()

    if isinstance(v, bool):
        return T.BooleanType()

    if isinstance(v, int):
        return T.LongType()

    if isinstance(v, float):
        return T.DoubleType()

    if isinstance(v, datetime):
        return T.TimestampType()

    if isinstance(v, date):
        return T.DateType()

    if isinstance(v, str):
        return T.StringType()

    if isinstance(v, list):

        t = T.NullType()

        for x in v:
            t = mesclar(t, tipo(x), 'array')

        return T.ArrayType(t, True)

    if isinstance(v, dict):

        return T.StructType([
            T.StructField(str(k), tipo(x), True)
            for k, x in v.items()
        ])

    raise TypeError(str(type(v)))


# ============================================================
# 7. MESCLAGEM DE SCHEMAS
# ============================================================

def mesclar(a, b, local):

    if isinstance(a, T.NullType):
        return b

    if isinstance(b, T.NullType):
        return a

    if (
        isinstance(a, T.StructType)
        and isinstance(b, T.StructType)
    ):

        da = {
            f.name: f.dataType
            for f in a
        }

        db = {
            f.name: f.dataType
            for f in b
        }

        return T.StructType([
            T.StructField(
                k,
                mesclar(
                    da.get(k, T.NullType()),
                    db.get(k, T.NullType()),
                    local + '.' + k
                ),
                True
            )
            for k in dict.fromkeys([*da, *db])
        ])

    if (
        isinstance(a, T.ArrayType)
        and isinstance(b, T.ArrayType)
    ):

        return T.ArrayType(
            mesclar(
                a.elementType,
                b.elementType,
                local + '[]'
            ),
            True
        )

    if (
        isinstance(a, (T.LongType, T.DoubleType))
        and isinstance(b, (T.LongType, T.DoubleType))
    ):

        return (
            T.DoubleType()
            if (
                isinstance(a, T.DoubleType)
                or isinstance(b, T.DoubleType)
            )
            else T.LongType()
        )

    if type(a) is type(b):
        return a

    raise TypeError(
        f'{local}: '
        f'{a.simpleString()} x {b.simpleString()}'
    )


# ============================================================
# 8. FINALIZACAO DO SCHEMA
# ============================================================

def finalizar(t):

    if isinstance(t, T.NullType):
        return T.StringType()

    if isinstance(t, T.ArrayType):

        return T.ArrayType(
            finalizar(t.elementType),
            True
        )

    if isinstance(t, T.StructType):

        return T.StructType([
            T.StructField(
                f.name,
                finalizar(f.dataType),
                True
            )
            for f in t
        ])

    return t


# ============================================================
# 9. ESTRUTURACAO DOS VALORES
# ============================================================

def estruturar(v, t):

    if v is None:
        return None

    if isinstance(t, T.StructType):

        if not isinstance(v, dict):
            raise TypeError('Struct esperado')

        return {
            f.name: estruturar(
                v.get(f.name),
                f.dataType
            )
            for f in t
        }

    if isinstance(t, T.ArrayType):

        if not isinstance(v, list):
            raise TypeError('Array esperado')

        return [
            estruturar(x, t.elementType)
            for x in v
        ]

    return v


# ============================================================
# 10. LEITURA E VALIDACAO DOS CSVs
# ============================================================

preparados = {}

with zipfile.ZipFile(ZIP) as z:

    arquivos = {}

    for n in z.namelist():

        if (
            n.lower().endswith('.csv')
            and not n.startswith('__MACOSX/')
        ):

            b = n.rsplit('/', 1)[-1][:-4]

            if b in arquivos:
                raise ValueError(
                    'CSV duplicado: ' + b
                )

            arquivos[b] = n

    if set(arquivos) != set(ESPERADO):

        raise ValueError(
            f'CSV faltando='
            f'{set(ESPERADO) - set(arquivos)}; '
            f'extras='
            f'{set(arquivos) - set(ESPERADO)}'
        )

    for base, esperado in ESPERADO.items():

        reader = csv.DictReader(
            io.StringIO(
                z.read(arquivos[base]).decode('utf-8-sig')
            ),
            delimiter='|'
        )

        cols = list(reader.fieldnames or [])

        if not cols or len(cols) != len(set(cols)):
            raise ValueError(
                base + ': cabecalho invalido'
            )

        rows = []

        tipos = {
            c: T.NullType()
            for c in cols
        }

        for numero, raw in enumerate(reader, 2):

            if None in raw:
                raise ValueError(
                    f'{base} linha {numero}: colunas extras'
                )

            r = {}

            for c in cols:

                try:

                    v = converter(
                        c,
                        raw[c]
                        if (
                            c in ('name', 'description')
                            and raw[c] not in (None, '')
                        )
                        else interpretar(raw[c])
                    )

                    if (
                        base == 'payment_methods'
                        and c == '_id'
                        and isinstance(v, dict)
                        and 'oid' in v
                    ):

                        if '$oid' in v:
                            raise ValueError(
                                'oid e $oid simultaneos'
                            )

                        v = {
                            '$oid' if k == 'oid' else k: x
                            for k, x in v.items()
                        }

                    r[c] = v

                    tipos[c] = mesclar(
                        tipos[c],
                        tipo(v),
                        f'{base}.{c} linha {numero}'
                    )

                except Exception as e:

                    raise ValueError(
                        f'{base} linha {numero} '
                        f'coluna {c}: {e}'
                    ) from e

            rows.append(r)

        if len(rows) != esperado:

            raise ValueError(
                f'{base}: {len(rows)} != {esperado}'
            )

        for c in cols:

            override = OVERRIDES.get(
                (base, c)
            )

            if override is not None:

                if (
                    not isinstance(
                        tipos[c],
                        T.NullType
                    )
                    and tipos[c] != override
                ):

                    raise TypeError(
                        f'{base}.{c}: tipo de dados '
                        f'diverge do contrato explicitado'
                    )

                tipos[c] = override

        schema = T.StructType([
            T.StructField(
                c,
                finalizar(tipos[c]),
                True
            )
            for c in cols
        ])

        preparados[base] = (
            [
                tuple(
                    estruturar(
                        r[c],
                        schema[c].dataType
                    )
                    for c in cols
                )
                for r in rows
            ],
            schema
        )

        print(
            'VALIDADO',
            base,
            len(rows),
            'linhas',
            len(cols),
            'colunas'
        )


# ============================================================
# 11. GRAVACAO PARQUET COM PARTICIONAMENTO OPCIONAL
# ============================================================

print()
print('TODOS OS CSVs VALIDADOS; iniciando escrita...')

print(
    'PARTICIONAMENTO:',
    'ATIVADO' if PARTICIONAMENTO else 'DESATIVADO'
)

if PARTICIONAMENTO:
    print('NUMERO DE PARTICOES:', NUM_PARTICOES)

print()

for base, (linhas, schema) in preparados.items():

    caminho = f'{DESTINO}/{base}'

    # Criacao do DataFrame original
    df = spark.createDataFrame(
        linhas,
        schema=schema
    )

    # --------------------------------------------------------
    # PARTICIONAMENTO OPCIONAL
    # --------------------------------------------------------

    if PARTICIONAMENTO:

        df = df.repartition(NUM_PARTICOES)

        print(
            'PARTICIONAMENTO |',
            base,
            '|',
            NUM_PARTICOES,
            'particoes'
        )

    else:

        print(
            'SEM REPARTITION EXPLICITO |',
            base
        )

    # --------------------------------------------------------
    # GRAVACAO PARQUET
    # --------------------------------------------------------

    df.write.mode(
        'overwrite'
        if SOBRESCREVER
        else 'errorifexists'
    ).parquet(caminho)

    # --------------------------------------------------------
    # VALIDACAO DA GRAVACAO
    # --------------------------------------------------------

    relido = spark.read.parquet(caminho)

    assert relido.count() == ESPERADO[base], (
        base + ' count divergente'
    )

    assert relido.schema == schema, (
        base + ' schema divergente'
    )

    print(
        'OK',
        base,
        ESPERADO[base],
        'linhas',
        len(relido.columns),
        'colunas'
    )

    print()


# ============================================================
# 12. RESULTADO FINAL
# ============================================================

print('=' * 65)
print('PROCESSAMENTO CONCLUIDO')
print('=' * 65)

print(
    'Particionamento:',
    'ATIVADO' if PARTICIONAMENTO else 'DESATIVADO'
)

if PARTICIONAMENTO:
    print('Numero de particoes:', NUM_PARTICOES)

print('Bases processadas:', len(ESPERADO))
print('Total de registros:', sum(ESPERADO.values()))
print('Destino:', DESTINO)

print('=' * 65)


# COMMAND ----------


# Consulta quantidades
from pyspark.sql import functions as F

BASE_PATH = f'{DESTINO_PARQUET}'

bases = [
    "base_prices_current",
    "campaigns_v2",
    "discounts_v2",
    "payment_methods",
    "prices",
    "prices_historic",
    "prices_penultimate_execution",
    "products_discounts",
    "robinhood_campaigns",
    "robinhood_campaign_products",
    "selling_prices2_current",
    "visao_catalogo_produto_atual"
]

resultados = []

for base in bases:
    caminho = f"{BASE_PATH}/{base}"

    try:
        df = spark.read.parquet(caminho)

        resultados.append((
            base,
            df.count(),
            len(df.columns),
            "OK"
        ))

    except Exception as e:
        resultados.append((
            base,
            0,
            0,
            "ERRO: " + str(e).split("\n")[0][:120]
        ))

df_resultado = spark.createDataFrame(
    resultados,
    ["base", "quantidade_registros", "quantidade_colunas", "status"]
)

display(df_resultado.orderBy("base"))

print(
    "TOTAL DE REGISTROS:",
    df_resultado.filter(F.col("status") == "OK")
    .agg(F.sum("quantidade_registros"))
    .first()[0] or 0
)


# COMMAND ----------


# Verifica Schema das Bases
BASE_PATH = f'{DESTINO_PARQUET}'

for base in bases:
    print("\n" + "=" * 80)
    print(f"BASE: {base}")
    print("=" * 80)

    try:
        df = spark.read.parquet(f"{BASE_PATH}/{base}")
        df.printSchema()

    except Exception as e:
        print("ERRO:", str(e).split("\n")[0])


# COMMAND ----------

# Databricks Python notebook - executar em UMA celula, apos 01.
# Somente leitura; nao reprova automaticamente duplicidades nem orfaos.
from pyspark.sql import functions as F, types as T
from functools import reduce

ROOT=f'{DESTINO_PARQUET}'
EXPECTED={'base_prices_current':(5,10),'campaigns_v2':(50,22),'discounts_v2':(24,24),
          'payment_methods':(18,9),'prices':(5,22),'prices_historic':(56,27),
          'prices_penultimate_execution':(5,22),'products_discounts':(39,11),
          'robinhood_campaign_products':(22,16),'robinhood_campaigns':(13,15),
          'selling_prices2_current':(5,11),'visao_catalogo_produto_atual':(10,58)}
frames={};checks=[]
def report(grupo,base,metrica,valor,esperado=None,status='INFO',detalhe=''):
    checks.append((grupo,base,metrica,str(valor),'' if esperado is None else str(esperado),status,detalhe))

for base,(n,c) in EXPECTED.items():
    try:
        df=spark.read.parquet(f'{ROOT}/{base}');frames[base]=df
        got=df.count(); cols=len(df.columns)
        report('01_ESTRUTURA',base,'linhas',got,n,'OK' if got==n else 'FALHA')
        report('01_ESTRUTURA',base,'colunas',cols,c,'OK' if cols==c else 'FALHA')
        report('01_ESTRUTURA',base,'nomes_duplicados',len(df.columns)-len(set(df.columns)),0,
               'OK' if len(df.columns)==len(set(df.columns)) else 'FALHA')
    except Exception as e:report('01_ESTRUTURA',base,'leitura',type(e).__name__,None,'FALHA',str(e)[:300])
assert len(frames)==12,'Uma ou mais bases nao foram lidas; verificar 01_ESTRUTURA'

# Contratos comprovados pelo uso no codigo legado: selections precisa ser array, e get_robinhood_campaigns faz join por campaign_id.
contratos=[('base_prices_current','selections','array<string>'),
          ('campaigns_v2','seller','array<string>'),('discounts_v2','seller','array<string>'),
          ('robinhood_campaign_products','_id','string'),
          ('visao_catalogo_produto_atual','attributes','array<string>'),
          ('visao_catalogo_produto_atual','stock','boolean')]
for base,col,esperado in contratos:
    got=frames[base].schema[col].dataType.simpleString()
    report('02_CONTRATO',base,col,got,esperado,'OK' if got==esperado else 'FALHA')
for base in ('campaigns_v2','discounts_v2','payment_methods','products_discounts','robinhood_campaigns'):
    tipo=frames[base].schema['_id'].dataType
    got=tipo.simpleString()
    status='OK' if isinstance(tipo,T.StructType) and '$oid' in tipo.fieldNames() else 'FALHA'
    report('02_CONTRATO',base,'_id.$oid',got,'struct com $oid',status)

# Nulos e arrays vazios por coluna top-level. Tipos de todos os campos registrados.
for base,df in frames.items():
    for field in df.schema.fields:
        c=field.name; t=field.dataType
        report('03_SCHEMA',base,c,t.simpleString(),None,'INFO','nullable='+str(field.nullable))
        q=F.col('`'+c.replace('`','``')+'`')
        nulos=df.filter(q.isNull()).count()
        if nulos:report('04_QUALIDADE',base,c+'.nulos',nulos,None,'INFO')
        if isinstance(t,T.ArrayType):
            vazios=df.filter(q.isNotNull() & (F.size(q)==0)).count()
            if vazios:report('04_QUALIDADE',base,c+'.arrays_vazios',vazios,None,'INFO')
        if isinstance(t,(T.DoubleType,T.FloatType)):
            invalidos=df.filter(F.isnan(q) | (F.abs(q)==float('inf'))).count()
            if invalidos:report('04_QUALIDADE',base,c+'.nan_inf',invalidos,0,'ATENCAO')

# Chaves candidatas: contar grupos repetidos, nao linhas duplicadas.
chaves={
 'base_prices_current':['seller_id','sku'],
 'selling_prices2_current':['seller_id','sku','channel_id','campaign_code'],
 'visao_catalogo_produto_atual':['seller_id','sku'],
 'products_discounts':['seller','sku','campaign_id'],
 'robinhood_campaign_products':['seller_id','sku','campaign_id'],
 'campaigns_v2':['id'],'discounts_v2':['id'],
 'payment_methods':['id'],'robinhood_campaigns':['campaign_id'],
 'prices':['seller_id','sku','campaign_code'],
 'prices_penultimate_execution':['seller_id','sku','campaign_code'],
 'prices_historic':['seller_id','sku','campaign_code','price_date']}
for base,cols in chaves.items():
    df=frames[base]
    nulls=df.filter(reduce(lambda a,b:a|b,[F.col(c).isNull() for c in cols])).count()
    counts=df.groupBy(*cols).count()
    duplicates=counts.filter(F.col('count')>1).count()
    max_mult=counts.agg(F.max('count')).first()[0]
    report('05_CHAVES',base,','.join(cols),duplicates,None,'INFO',f'linhas_chave_nula={nulls}; max_multiplicidade={max_mult}')

# Relacionamentos fisicos; comparacao sempre explicita, sem inferir integridade.
rels=[
 ('base_prices_current','selling_prices2_current',[('seller_id','seller_id'),('sku','sku')]),
 ('base_prices_current','visao_catalogo_produto_atual',[('seller_id','seller_id'),('sku','sku')]),
 ('products_discounts','visao_catalogo_produto_atual',[('seller','seller_id'),('sku','sku')]),
 ('robinhood_campaign_products','visao_catalogo_produto_atual',[('seller_id','seller_id'),('sku','sku')]),
 ('robinhood_campaign_products','robinhood_campaigns',[('campaign_id','campaign_id')]),
 ('products_discounts','discounts_v2',[('campaign_id','id')]),
 ('prices','visao_catalogo_produto_atual',[('seller_id','seller_id'),('sku','sku')]),
 ('prices_historic','visao_catalogo_produto_atual',[('seller_id','seller_id'),('sku','sku')]),
 ('prices_penultimate_execution','prices',[('seller_id','seller_id'),('sku','sku'),('campaign_code','campaign_code')]),
]
for src,dst,pairs in rels:
    a=frames[src].alias('a');b=frames[dst].alias('b')
    cond=reduce(lambda x,y:x&y,[(F.col('a.`'+x+'`')==F.col('b.`'+y+'`')) for x,y in pairs])
    matched=a.join(b,cond,'inner').count()
    orphans=a.join(b,cond,'left_anti').count()
    report('06_JOINS',src+' -> '+dst,','.join(x+'='+y for x,y in pairs),matched,None,'INFO',f'linhas_orfas={orphans}; origem={a.count()}; destino={b.count()}')

# Diagnostico especifico do contrato original get_robinhood_campaigns:
# benefit='loyalty', data atual entre start_date/end_date, action='insert'.
c=frames['robinhood_campaigns'].filter(
    (F.col('benefit')=='loyalty') &
    F.current_timestamp().between(F.col('start_date'),F.col('end_date'))
)
p=frames['robinhood_campaign_products'].filter(F.col('action')=='insert')
report('07_ROBINHOOD','robinhood_campaigns','campanhas_elegiveis',c.count(),None,'INFO')
report('07_ROBINHOOD','robinhood_campaign_products','produtos_action_insert',p.count(),None,'INFO')
report('07_ROBINHOOD','join_original_campaign_id','linhas',c.join(p,'campaign_id','inner').count(),None,'INFO')
report('07_ROBINHOOD','robinhood_campaigns','benefit_distribuicao',str([x.asDict() for x in frames['robinhood_campaigns'].groupBy('benefit').count().collect()]),None,'INFO')
report('07_ROBINHOOD','robinhood_campaign_products','action_distribuicao',str([x.asDict() for x in frames['robinhood_campaign_products'].groupBy('action').count().collect()]),None,'INFO')

# Evidencias de IDs originais, nao pressupor que orfaos sao erro.
for base,col in [('robinhood_campaigns','campaign_id'),('robinhood_campaign_products','campaign_id'),
                 ('discounts_v2','id'),('products_discounts','campaign_id')]:
    report('08_IDENTIFICADORES',base,col+'.distintos',frames[base].select(col).distinct().count(),None,'INFO')

result=spark.createDataFrame(checks,'grupo string, base string, metrica string, valor string, esperado string, status string, detalhe string')
display(result.groupBy('grupo','status').count().orderBy('grupo','status'))
display(result.filter(F.col('status')=='FALHA'))
display(result.filter(F.col('grupo').isin('05_CHAVES','06_JOINS','07_ROBINHOOD')).orderBy('grupo','base'))
print('BASES',len(frames),'REGISTROS',sum(x.count() for x in frames.values()))
print('RESULTADO:', 'COM FALHAS' if result.filter(F.col('status')=='FALHA').count() else 'ESTRUTURA E CONTRATOS TESTADOS OK; relacoes INFO requerem analise AS IS')
# Apenas relatorio, nao modifica bases originais.
RELATORIO=f'DESTINO_RELATORIO'
result.write.mode('overwrite').option('header',True).csv(RELATORIO+'/testes')
print('Relatorio salvo:',RELATORIO+'/testes')