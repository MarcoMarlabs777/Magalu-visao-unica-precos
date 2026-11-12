def get_partition_column(options):
    """
    Devolve uma lista das partições indicadas no dataset
    """

    partition_columns = []

    if "partitionBy" in options.keys():
        if not isinstance(options["partitionBy"], list):
            partition_columns.append(options["partitionBy"])
        else:
            partition_columns = options["partitionBy"]

    return partition_columns


def get_schema_ddl(df, partition_columns, format_data):
    """
    Monta as colunas para o CREATE TABLE
    de acordo com formato dos dados(delta ou parquet)
    """

    schema_ddl_partition = ""
    schema_ddl = ""

    list_columns_types = (
        df.schema.simpleString()
        .replace('struct<', "")
        .replace(">", "")
        .split(',')
    )

    if partition_columns:

        for column in list_columns_types:
            column_name, column_type = column.split(':')
            if column_name in partition_columns:
                if format_data == 'parquet':
                    df = df.drop(column_name)
                    schema_ddl_partition += "`{}` {},".format(
                        column_name, column_type
                    )
                else:
                    schema_ddl_partition += "{},".format(column_name)

        schema_ddl_partition = schema_ddl_partition.rstrip(',')

    schema_ddl = df._jdf.schema().toDDL()
    return schema_ddl, schema_ddl_partition


def save_metadata(
    spark_session, df, environment, zone, namespace, dataset, path, options
):
    """
    Recebe os parâmetros e monta o CREATE TABLE
    Gera uma EXTERNAL TABLE no HIVE METASTORE
    """

    if (
        zone in ('trusted', 'safe', 'refined')
        and 'format' in options.keys()
        and options["format"] in ('delta', 'parquet')
        and not spark_session.version.startswith("2")
    ):

        format_data = options["format"]

        zone = zone if type(zone) == str else zone.value
        environment = (
            environment if type(environment) == str else environment.value
        )

        partition_columns = get_partition_column(options)
        schema_ddl, schema_ddl_partition = get_schema_ddl(
            df, partition_columns, format_data
        )

        database_name = (
            namespace
            if environment == "prd"
            else "{}_{}".format(namespace, environment)
        )
        database_name = (
            database_name
            if zone == "trusted"
            else "{}_{}".format(database_name, zone)
        )
        table_name = dataset

        spark_session.sql(
            "CREATE DATABASE IF NOT EXISTS {}".format(database_name)
        )
        spark_session.sql(
            "DROP TABLE IF EXISTS {}.{}".format(database_name, table_name)
        )

        create_table = "CREATE TABLE {}.{} ({}) ".format(
            database_name, table_name, schema_ddl
        )

        if format_data == "delta":
            create_table += "USING DELTA "
        else:
            create_table += "STORED AS PARQUET "

        if partition_columns:
            create_table += "PARTITIONED BY ({}) ".format(schema_ddl_partition)

        create_table += "LOCATION '{}'".format(path)

        print("DDL: ", create_table)
        spark_session.sql(create_table)

        if partition_columns and format_data == "parquet":
            spark_session.sql(
                "ALTER TABLE {}.{} RECOVER PARTITIONS".format(
                    database_name, table_name
                )
            )
