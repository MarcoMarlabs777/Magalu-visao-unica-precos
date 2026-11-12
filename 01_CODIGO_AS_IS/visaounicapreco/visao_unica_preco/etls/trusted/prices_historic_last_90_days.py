from typing import Optional, Sequence

import pyspark.sql.types as st
from pyspark.sql import functions as sf
from sness.datalake.metadata import Zone
from utils import config_spark, parse_args, read_visao_unica_dataset
import os
from functools import reduce


def main(argv: Optional[Sequence[str]] = None) -> int:

    ss = config_spark()
    args = parse_args()
    ZONE = Zone.REFINED
    NAMESPACE_TO_WRITE = "visao_unica"

    base = "/Volumes/workspace/default/dados_teste_visao_unica"
    caminho = f"{base}/resultados/prices_historic"

    schema = ss.spark.read.parquet(
        f"{base}/parquet/prices_historic"
    ).schema

    arquivos = sorted(
        os.path.join(caminho, nome)
        for nome in os.listdir(caminho)
        if nome.endswith(".parquet")
    )

    if not arquivos:
        raise ValueError("Nenhum arquivo Parquet encontrado")

    dataframes = []

    for arquivo in arquivos:
        df = ss.spark.read.parquet(arquivo)

        df = df.select(*[
            (
                sf.col(c.name).cast(c.dataType)
                if c.name in df.columns
                else sf.lit(None).cast(c.dataType)
            ).alias(c.name)
            for c in schema.fields
        ])

        dataframes.append(df)

    df_prices_historic = reduce(
        lambda a, b: a.unionByName(b),
        dataframes
    ).filter(
        sf.col("price_date")
        >= sf.date_sub(sf.current_date(), args.days_to_process)
    )

    df_prices_historic.cache()

    ss.write.parquet(
        df_prices_historic,
        args.environment,
        ZONE,
        NAMESPACE_TO_WRITE,
        "prices_historic_last_90_days",
        partitionBy="price_date",
        mode="overwrite",
        mergeSchema=True,
    )


if __name__ == "__main__":
    main()
