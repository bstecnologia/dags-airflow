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

dag = DAG('CARGA_DE_DADOS_ATD_ARAXA_BASE_REPLICA', description='DAG para realizar a migração de araxa ',
          schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False)
operacionais = TaskGroup('OPERACIONAIS', dag=dag)
sem_relacionamento = TaskGroup('SEM_RELACIONAMENTO', dag=dag)
relacionamento_n1 = TaskGroup('RELACIONAMENTO_N1', dag=dag)
relacionamento_n2 = TaskGroup('RELACIONAMENTO_N2', dag=dag)
relacionamento_n3 = TaskGroup('RELACIONAMENTO_N3', dag=dag)
relacionamento_n4 = TaskGroup('RELACIONAMENTO_N4', dag=dag)
relacionamento_n5 = TaskGroup('RELACIONAMENTO_N5', dag=dag)

jars = os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),

gerar_sequencial = SparkSubmitOperator(
    task_id='GERAR_SEQUENCIAL',
    conn_id='spark',
    application=os.path.join(current_dir+'/migracao_csv', 'gerador_sequencia.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[str(sequencia_uuid)],
    dag=dag,
    task_group=operacionais
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_cfg_municipios.json'))
sce_cfg_municipios = SparkSubmitOperator(
    task_id='SCE_CFG_MUNICIPIOS',
    conn_id='spark',
    application=os.path.join(current_dir+'/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    dag=dag,
    task_group=relacionamento_n1

)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_abrangencias.json'))
sce_abrangencias = SparkSubmitOperator(
    task_id='SCE_ABRANGENCIAS',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=sem_relacionamento,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_abrangencias_municipios.json'))
sce_abrangencias_municipios = SparkSubmitOperator(
    task_id='SCE_ABRANGENCIAS_MUNICIPIOS',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n2,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_cfg_especialidades.json'))
sce_cfg_especialidades = SparkSubmitOperator(
    task_id='SCE_CFG_ESPECIALIDADES',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=sem_relacionamento,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_cfg_ufs.json'))
sce_cfg_ufs = SparkSubmitOperator(
    task_id='SCE_CFG_UFS',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=sem_relacionamento,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_cfg_feriados.json'))
sce_feriados= SparkSubmitOperator(
    task_id='SCE_FERIADOS',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=sem_relacionamento,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_pessoas.json'))
sce_pessoas = SparkSubmitOperator(
    task_id='SCE_PESSOAS',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=sem_relacionamento,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_cfg_itens.json'))
sce_cfg_itens = SparkSubmitOperator(
    task_id='SCE_CFG_ITENS',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=sem_relacionamento,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_cfg_itens_tipo.json'))
sce_cfg_itens_tipos = SparkSubmitOperator(
    task_id='SCE_CFG_ITENS_TIPOS',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=sem_relacionamento,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_cfg_itens_tipo_versoes.json'))
sce_cfg_itens_tipos_versoes = SparkSubmitOperator(
    task_id='SCE_CFG_ITENS_TIPOS_VERSOES',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n1,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_cfg_fornecedores.json'))
sce_cfg_fornecedores= SparkSubmitOperator(
    task_id='SCE_CFG_FORNECEDORES',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=sem_relacionamento,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_cfg_rede_atend.json'))
sce_cfg_rede_atend = SparkSubmitOperator(
    task_id='SCE_CFG_REDE_ATEND',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=sem_relacionamento,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_cfg_erros.json'))
sce_cfg_erros = SparkSubmitOperator(
    task_id='SCE_CFG_ERROS',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n2,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_grupos.json'))
sce_grupos = SparkSubmitOperator(
    task_id='SCE_GRUPOS',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=sem_relacionamento,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_cfg_cids.json'))
sce_cfg_cids = SparkSubmitOperator(
    task_id='SCE_CFG_CIDS',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=sem_relacionamento,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_grupos_contratos.json'))
sce_grupos_contratos = SparkSubmitOperator(
    task_id='SCE_GRUPOS_CONTRATOS',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n5,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_grupos_itens.json'))
sce_grupos_itens = SparkSubmitOperator(
    task_id='SCE_GRUPOS_ITENS',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n1,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_pacotes.json'))
sce_pacotes = SparkSubmitOperator(
    task_id='SCE_PACOTES',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n4,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_pacotes_itens.json'))
sce_pacotes_itens = SparkSubmitOperator(
    task_id='SCE_PACOTES_ITENS',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n5,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_grupos_prestadores.json'))
sce_grupos_prestadores = SparkSubmitOperator(
    task_id='SCE_GRUPOS_PRESTADORES',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n4,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_pessoas_enderecos.json'))
sce_pessoas_enderecos = SparkSubmitOperator(
    task_id='SCE_PESSOAS_ENDERECOS',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar') + "," + os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n2,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_pessoas_end_contat.json'))
sce_pessoas_end_contat = SparkSubmitOperator(
    task_id='SCE_PESSOAS_END_CONTAT',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n3,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_planos.json'))
sce_planos = SparkSubmitOperator(
    task_id='SCE_PLANOS',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n1,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_planos_cob_excecoes.json'))
sce_planos_cob_excecoes = SparkSubmitOperator(
    task_id='SCE_PLANOS_COB_EXCECOES',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n2,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_planos_cobertura.json'))
sce_planos_cobertura = SparkSubmitOperator(
    task_id='SCE_PLANOS_COBERTURA',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n2,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_prestadores.json'))
sce_prestadores = SparkSubmitOperator(
    task_id='SCE_PRESTADORES',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n3,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_tab_precos.json'))
sce_tab_precos = SparkSubmitOperator(
    task_id='SCE_TAB_PRECOS',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=sem_relacionamento,
    dag=dag
)
"""
dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_empresa.json'))
sce_empresa = SparkSubmitOperator(
    task_id='SCE_EMPRESA',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar') + "," + os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar'),
    application_args=[json.dumps(dados)],
    task_group=sem_relacionamento,
    dag=dag
)
"""
dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_empresa_agrmto.json'))
sce_empresa_agrmto = SparkSubmitOperator(
    task_id='SCE_EMPRESA_AGRMTO',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n1,
    dag=dag
)



dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_prest_especialidades.json'))
sce_prest_especialidades = SparkSubmitOperator(
    task_id='SCE_PREST_ESPECIALIDADES',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n4,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_prest_rede_atend.json'))
sce_prest_rede_atend = SparkSubmitOperator(
    task_id='SCE_PREST_REDE_ATEND',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n4,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_prest_rede_atend_parcial.json'))
sce_prest_rede_atend_parcial = SparkSubmitOperator(
    task_id='SCE_PREST_REDE_ATEND_PARCIAL',
    conn_id='spark',
    application=os.path.join(current_dir + '/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n5,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_cfg_unimed.json'))
sce_cfg_unimeds= SparkSubmitOperator(
    task_id='SCE_CFG_UNIMEDS',
    conn_id='spark',
    application=os.path.join(current_dir+'/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n2,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_abrangencias_carencias.json'))
sce_abrangencias_carencias = SparkSubmitOperator(
    task_id='SCE_ABRANGENCIAS_CARENCIAS',
    conn_id='spark',
    application=os.path.join(current_dir+'/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n3,
    dag=dag
)

##TODO: verificar depois a falta de SCE_CFG_CIDS
dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_benef_cpt.json'))
sce_benef_cpt = SparkSubmitOperator(
    task_id='SCE_BENEF_CPT',
    conn_id='spark',
    application=os.path.join(current_dir+'/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n4,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_beneficiario_repasse.json'))
sce_beneficiario_repasse = SparkSubmitOperator(
    task_id='SCE_BENEFICIARIO_REPASSE',
    conn_id='spark',
    application=os.path.join(current_dir+'/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n4,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_beneficiarios.json'))
sce_beneficiarios = SparkSubmitOperator(
    task_id='SCE_BENEFICIARIOS',
    conn_id='spark',
    application=os.path.join(current_dir+'/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n4,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_contratos_venda.json'))
sce_contratos_venda = SparkSubmitOperator(
    task_id='SCE_CONTRATOS_VENDA',
    conn_id='spark',
    application=os.path.join(current_dir+'/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n3,
    dag=dag
)

"""
dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_carencias.json'))
sce_carencias = SparkSubmitOperator(
    task_id='SCE_CARENCIAS',
    conn_id='spark',
    application=os.path.join(current_dir+'/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n5,
    dag=dag
)
"""

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_cfg_pacote_princ.json'))
sce_pacote_princ = SparkSubmitOperator(
    task_id='SCE_CFG_PACOTE_PRINC',
    conn_id='spark',
    application=os.path.join(current_dir+'/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n2,
    dag=dag
)

dados = carrega_arquivo_json(os.path.join(current_dir, 'events', 'event_sce_contratos_coberturas.json'))
sce_contratos_coberturas = SparkSubmitOperator(
    task_id='SCE_CONTRATOS_COBERTURAS',
    conn_id='spark',
    application=os.path.join(current_dir+'/migracao_csv', 'migrador.py'),
    jars=os.path.join(current_dir, 'postgresql-42.2.22.jar')+","+os.path.join(current_dir, 'ojdbc8-19.3.0.0.jar')+","+os.path.join(current_dir, 'mssql-jdbc-12.4.2.jre11.jar'),
    application_args=[json.dumps(dados)],
    task_group=relacionamento_n5,
    dag=dag
)


operacionais>>sem_relacionamento >> relacionamento_n1 >> relacionamento_n2 >> relacionamento_n3 >> relacionamento_n4 >> relacionamento_n5


