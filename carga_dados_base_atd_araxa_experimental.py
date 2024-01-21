import os
from airflow import DAG
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
import uuid

import json

sequencia_uuid = uuid.uuid4()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

def carrega_arquivo_json(arquivo):
    with open(arquivo, 'r') as arquivo:
        dados = json.load(arquivo)
    return dados

current_dir = os.path.dirname(os.path.abspath(__file__))

dag = DAG('CARGA_DE_DADOS_ATD_ARAXA_EXPERIMENTAL', description='DAG para realizar a migração de araxa ',
          schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False)

operacionais = TaskGroup('OPERACIONAIS', dag=dag)

spark_submit_conf = [
    "--conf", "spark.driver.host=10.97.62.143"
]

##7
dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_cfg_especialidades.json'))
sce_cfg_especialidades = SparkSubmitOperator(
    task_id='SCE_CFG_ESPECIALIDADES',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    dag=dag
)



operacionais >> sce_cfg_especialidades
