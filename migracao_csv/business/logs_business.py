from helper.spark_helper import get_session, get_dataframame_query_database, save_log_dataframe,save_dataframe_database
from helper.data_base_helper import get_data_base_acess_airflow
from pyspark.sql.functions import *
import requests
import json

def get_sequence(sequencial_uuid):
    query = f"SELECT sequencia FROM controle_migracao where id = '{sequencial_uuid}'"
    print(query)

    df = get_dataframame_query_database(get_session(), query,
                                        get_data_base_acess_airflow())

    if df.count() == 0:
        df_sequencia = get_dataframame_query_database(get_session(), "SELECT nextval('s_controle_migracao') as sequence ",
                                            get_data_base_acess_airflow())
        sequence = df_sequencia.collect()[0]['sequence']

        data = [
            (sequencial_uuid, sequence)
        ]
        columns = ["id", "sequencia"]
        dataframe_execucao = get_session().createDataFrame(data, columns)
        dataframe_execucao = dataframe_execucao.withColumn("data_execucao", current_timestamp())
        save_dataframe_database(dataframe_execucao, 'controle_migracao', get_data_base_acess_airflow())
        return sequence
    else:
        sequence = df.collect()[0]['sequencia']
        return sequence

def get_max_sequence():
    query = f"SELECT max(sequencia) as sequencia FROM controle_migracao"

    df = get_dataframame_query_database(get_session(), query,
                                        get_data_base_acess_airflow())
    sequence = df.collect()[0]['sequencia']
    return sequence

async def send_data_from_database(data_frame, tabela_a_ser_migrada, tipo_log, sequencia):
    url = 'http://10.0.0.169:9096/log'

    if data_frame.count() > 0:
        df_json = data_frame.toJSON()
        for linha_json in df_json.collect():
            response = requests.post(url, json={"key":tabela_a_ser_migrada,
                                                "sequencia": sequencia,
                                                "data": {"tabela_a_ser_migrada": tabela_a_ser_migrada,
                                                         "tipo_log": tipo_log,
                                                         "dados": json.loads(linha_json)}})
            print(response.status_code)


