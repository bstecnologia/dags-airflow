from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re
import os

current_dir = os.path.dirname(os.path.abspath('airflow'))

def get_session():
    spark = (SparkSession.builder
             .appName("nova-data")
             .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
             .config("spark.driver.host", "10.98.96.8")
             .config("spark.driver.port", "43031")
             .getOrCreate()
             )
    return spark

def retornar_data_frame_spark(session,file_name, sepatator):
    arquivos_dir = os.path.join(current_dir, 'dags', 'migracao_csv', 'arquivos')
    df = session.read.load(os.path.join(arquivos_dir, f"/opt/spark-data/arquivos/{file_name}.csv"), format="csv",
                               inferSchema="true", sep=sepatator, header="true"
                               )
    return df


def get_df_spark(session, pandas_df):
    df = session.createDataFrame(pandas_df)
    df.show()
    return df

def converter_data_frame_pandas_em_spark(session, pandas_df):
    df = session.createDataFrame(pandas_df)
    df.show()
    return df

def create_table_temp(data_frame, table_name):
    data_frame.createOrReplaceTempView(table_name)

def save_dataframe_database(data, table_destiny, class_helper):
    data.write.jdbc(url=class_helper['url'],
                    table=table_destiny,
                    mode="append",
                    properties=class_helper['properties'])


def get_dataframame_database(sesion, table_name, class_helper):
    return sesion.read.format("jdbc").option("url", class_helper['url']) \
        .option("dbtable", table_name).option("user", class_helper['properties']['user']) \
        .option("password", class_helper['properties']['password']).option("driver", class_helper['properties']['driver']).load()


def get_dataframame_query_database(sesion, table_name, class_helper):
    return sesion.read.format("jdbc").option("url", class_helper['url']) \
        .option("query", table_name).option("user", class_helper['properties']['user']) \
        .option("password", class_helper['properties']['password']).option("driver", class_helper['properties']['driver']).load()

def get_dataframe_from_query(sesion, query):
    return sesion.sql(query)

def set_data_datatypes(data_for_data_base,data):
    tipos_colunas = data_for_data_base.dtypes
    for coluna, tipo in tipos_colunas:
        if tipo == 'timestamp':
            data = data.withColumn(coluna, when(col(coluna).isNotNull(), to_date(col(coluna), "dd/MM/yyyy")))
    return data

def seta_mascara_de_datas_no_dataframe(data_for_data_base, data):
    tipos_colunas = data_for_data_base.dtypes
    all_columns = data.columns
    for coluna, tipo in tipos_colunas:
        if coluna in all_columns:
            if tipo == 'timestamp':
                data = data.withColumn(coluna, when(col(coluna).isNotNull(), to_date(col(coluna), "dd/MM/yyyy")))
            elif tipo.startswith('decimal'):
                data = data.withColumn(coluna, col(coluna).cast(tipo))  # Ajuste o tipo conforme necessário
        else:
            data = data.withColumn(coluna, lit(None).cast(tipo))
    return data

def get_dataframe_for_estutural(session, table_name, class_helper):
    return session.read.format("jdbc").option("url", class_helper['url']) \
        .option("query"," SELECT DATA_LENGTH, COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = '"+table_name+"' AND DATA_TYPE = 'VARCHAR2'" )\
        .option("user", class_helper['properties']['user']) \
        .option("password", class_helper['properties']['password']).option("driver", class_helper['properties']['driver']).load()

def get_constraints_type_check(session, table_name, class_helper):
    return session.read.format("jdbc").option("url", class_helper['url']) \
        .option("query",f"SELECT CONSTRAINT_NAME, SEARCH_CONDITION "
                        f"FROM USER_CONSTRAINTS WHERE TABLE_NAME = '{table_name}' AND CONSTRAINT_TYPE = 'C' " )\
        .option("user", class_helper['properties']['user']) \
        .option("password", class_helper['properties']['password']).option("driver", class_helper['properties']['driver']).load()

def retorna_tamanho_das_colunas(session, table_name, class_helper):
    return session.read.format("jdbc").option("url", class_helper['url']) \
        .option("query"," SELECT DATA_LENGTH, COLUMN_NAME FROM ALL_TAB_COLUMNS "
                        "WHERE TABLE_NAME = '"+table_name+"' AND DATA_TYPE = 'VARCHAR2'" )\
        .option("user", class_helper['properties']['user']) \
        .option("password", class_helper['properties']['password']).option("driver", class_helper['properties']['driver']).load()

def retorna_valor_default_colunas(session, table_name, class_helper):
    return session.read.format("jdbc").option("url", class_helper['url']) \
        .option("query"," SELECT DATA_DEFAULT, COLUMN_NAME FROM ALL_TAB_COLUMNS "
                        "WHERE TABLE_NAME = '"+table_name+"' AND DATA_DEFAULT IS NOT NULL AND DATA_TYPE IN('VARCHAR2','NUMBER')" )\
        .option("user", class_helper['properties']['user']) \
        .option("password", class_helper['properties']['password']).option("driver", class_helper['properties']['driver']).load()

def seta_valor_default_das_colunas(data_for_data_base, data):
    for row in data_for_data_base.collect():
        print(row['COLUMN_NAME'], row['DATA_DEFAULT'].replace("'", ""))
        data = data.withColumn(row['COLUMN_NAME'],
                               when(col(row['COLUMN_NAME']).isNull() | (col(row['COLUMN_NAME']) == ""), row['DATA_DEFAULT'].replace("'", "")).otherwise(col(row['COLUMN_NAME'])))
    return data

def monta_join_com_tabela(tabela_principal,tabela_join ,colunas_tabela_principal, colunas_tabela_join):
    query = f" SELECT {tabela_principal}.* FROM {tabela_principal} , {tabela_join} WHERE"
    contador = 0
    for coluna, coluna_join in zip(colunas_tabela_principal, colunas_tabela_join):
        if contador == 0:
            query = query + f" {tabela_principal}.{coluna} = {tabela_join}.{coluna_join} "
        else:
            query = query + f" AND {tabela_principal}.{coluna} = {tabela_join}.{coluna_join} "
        contador = contador + 1

    return query

def retorna_registros_com_chave_duplicada(data, coluna):
    return data.groupBy(coluna).count().where("count > 1")

def remove_registros_com_chave_duplicada(data,registros_duplicados, coluna):
    duplicated_cnes = [row[coluna] for row in registros_duplicados.collect()]
    data = data.filter(~col(coluna).isin(duplicated_cnes))
    return data

def monta_registro_nao_encontrado(tabela_principal,tabela_join ,colunas_tabela_principal, colunas_tabela_join):
    query = f" SELECT {tabela_principal}.* FROM {tabela_principal} WHERE "
    contador = 0
    for coluna in colunas_tabela_principal:
        if contador == 0:
            query = query + f" ( {coluna} "
        else:
            query = query + f" , {coluna} "
        contador = contador + 1

    query = query + f" ) NOT IN ( SELECT  "

    contador = 0
    for coluna in colunas_tabela_join:
        if contador == 0:
            query = query + f" {coluna} "
        else:
            query = query + f" , {coluna} "
        contador = contador + 1

    query = query + f" FROM {tabela_join} ) "

    return query


def filtrar_registros_com_tamanho_excedido(data, df_tamanhos_colunas):
    where = None
    contador = 0
    for row in df_tamanhos_colunas.collect():
        coluna = row['COLUMN_NAME']
        tamanho_esperado = int(row['DATA_LENGTH'])
        if contador == 0:
            where = f" LENGTH({coluna}) <= {tamanho_esperado} "
        else:
            where = where + f" AND LENGTH({coluna}) <= {tamanho_esperado} "
        contador = contador + 1
    if contador > 0:
        data = data.filter(where)

    return data

def filtrar_registros_que_n_atendem_a_constraints(data, constraint):
    condicao = constraint['SEARCH_CONDITION']
    data_no_contraint = None
    if " IN " in condicao:
        condicao = condicao.replace(" IN ", " NOT IN ")
        data_no_contraint = data.filter(condicao)
    return data_no_contraint

def retorna_filtro_registros_tamanho_correto(df_tamanhos_colunas):
    where = None
    contador = 0
    for row in df_tamanhos_colunas.collect():
        coluna = row['COLUMN_NAME']
        tamanho_esperado = int(row['DATA_LENGTH'])
        if contador == 0:
            where = f" length(coalesce({coluna},'')) <= {tamanho_esperado} "
        else:
            where = where + f" AND length(coalesce({coluna},'')) <= {tamanho_esperado} "
        contador = contador + 1
    return where

def retorna_filtro_registros_tamanho_excedido(df_tamanhos_colunas):
    where = None
    contador = 0
    for row in df_tamanhos_colunas.collect():
        coluna = row['COLUMN_NAME']
        tamanho_esperado = int(row['DATA_LENGTH'])
        if contador == 0:
            where = f" LENGTH({coluna}) > {tamanho_esperado} "
        else:
            where = where + f" OR LENGTH({coluna}) > {tamanho_esperado} "
        contador = contador + 1
        print(where)
    return where

def save_log_dataframe(session,name_table,tipo_operacao, tabela_join, quantidade_registros, sequencia):
    data = [
        (name_table, tipo_operacao, tabela_join, quantidade_registros,sequencia)
    ]
    columns = ["nome_tabela", "tipo_operacao", "tabela_join", "quantidade_registros", "sequencia"]
    df = session.createDataFrame(data, columns)
    df = df.withColumn("data_registro", current_timestamp())
    return df

def ajustar_consulta_sql(consulta):
    padrao = re.compile(r"(\w+)\s+IS\s+NULL", re.IGNORECASE)
    consulta_ajustada = padrao.sub(
        lambda match: f"{match.group(1)} IS NULL ",
        consulta
    )
    return consulta_ajustada

def adicionar_valor_vazio(consulta):
    padrao = re.compile(r"(IN\s*\([\s']*[^()]*?['\s]*\))", re.IGNORECASE)
    consulta_ajustada = padrao.sub(
        lambda match: f"{match.group(1)[:-1]})",
        consulta
    )

    return consulta_ajustada

def adicionar_null_condition(condicao):
    padrao = r'(\b\w+\b\s+IN\s*\([^)]+\))'
    padrao_compilado = re.compile(padrao, re.IGNORECASE)

    def substituir(m):
        campo_in = m.group(0)
        campo = re.search(r'\b\w+\b', campo_in).group()
        return f"({campo_in} OR {campo} IS NULL)"

    nova_condicao = re.sub(padrao_compilado, substituir, condicao)

    return nova_condicao
