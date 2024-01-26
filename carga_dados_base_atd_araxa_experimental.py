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

dag = DAG('CARGA_DE_DADOS_ATD_ARAXA', description='DAG para realizar a migração de araxa ',
          schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False)

operacionais = TaskGroup('OPERACIONAIS', dag=dag)

gerar_sequencial = SparkSubmitOperator(
    task_id='GERAR_SEQUENCIAL',
    conn_id='spark',
    application=os.path.join(current_dir+'/migracao_csv', 'gerador_sequencia.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[str(sequencia_uuid)],
    dag=dag,
    task_group=operacionais
)

##01-1
dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_grupos_prestadores.json'))
sce_cfg_especialidades = SparkSubmitOperator(
    task_id='SCE_GRUPOS_PRESTADORES',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    dag=dag
)

##02
dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_his_emis_cart.json'))
event_sce_his_emis_cart = SparkSubmitOperator(
    task_id='SCE_HIS_EMIS_CART',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    dag=dag
)

##02
dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_benef_titulos.json'))
event_sce_benef_titulos = SparkSubmitOperator(
    task_id='SCE_BENEF_TITULOS',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_contratos_coberturas.json'))
sce_contratos_coberturas = SparkSubmitOperator(
    task_id='SCE_CONTRATOS_COBERTURAS',
    conn_id='spark',
    application=os.path.join(current_dir+'/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_prest_end_vinc.json'))
event_sce_prest_end_vinc = SparkSubmitOperator(
    task_id='SCE_PREST_END_VINC',
    conn_id='spark',
    application=os.path.join(current_dir+'/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_prest_rede_atend_parcial.json'))
sce_prest_rede_atend_parcial = SparkSubmitOperator(
    task_id='SCE_PREST_REDE_ATEND_PARCIAL',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_beneficiario_repasse.json'))
sce_beneficiario_repasse = SparkSubmitOperator(
    task_id='SCE_BENEFICIARIO_REPASSE',
    conn_id='spark',
    application=os.path.join(current_dir+'/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    dag=dag
)


operacionais >> sce_cfg_especialidades >> event_sce_his_emis_cart >> event_sce_benef_titulos >> sce_contratos_coberturas \
>> event_sce_prest_end_vinc >> sce_prest_rede_atend_parcial >> sce_beneficiario_repasse
