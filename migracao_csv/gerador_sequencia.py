import sys
import json
from helper.spark_helper import get_session , get_dataframame_database, \
    seta_mascara_de_datas_no_dataframe, retorna_tamanho_das_colunas, retorna_filtro_registros_tamanho_correto, \
    retorna_filtro_registros_tamanho_excedido, create_table_temp, monta_join_com_tabela, \
    monta_registro_nao_encontrado, get_dataframe_from_query, save_dataframe_database, save_log_dataframe, \
    retorna_registros_com_chave_duplicada, remove_registros_com_chave_duplicada, get_constraints_type_check, \
    filtrar_registros_que_n_atendem_a_constraints, ajustar_consulta_sql, adicionar_null_condition, \
    retornar_data_frame_spark, retorna_valor_default_colunas,seta_valor_default_das_colunas
from helper.log_helper import logger_info,logger_warning,logger_error
from helper.data_base_helper import get_data_base_acess_integracao_replica, get_data_base_acess_airflow
from helper.logger_helper import setup_logger
from business.logs_business import get_sequence, send_data_from_database
import asyncio

if __name__ == '__main__':
    sequencia = sys.argv[1]
    get_sequence(sequencia)








