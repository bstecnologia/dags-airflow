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
from helper.data_base_helper import get_data_base_acess_integracao_replica, get_data_base_acess_airflow, \
    get_data_base_replica_sql_server, get_data_base_acess_atd_araxa
from helper.logger_helper import setup_logger
from business.logs_business import get_max_sequence, send_data_from_database
import asyncio

if __name__ == '__main__':

    file_for_migration = sys.argv[1]
    dado_a_ser_migrado = json.loads(file_for_migration)

    tabela_a_ser_migrada = dado_a_ser_migrado['tabela_a_ser_migrada']

    logger = setup_logger(tabela_a_ser_migrada)
    sequence = get_max_sequence()

    logger.warning(
        f"SEQUENCIA:: {sequence}")

    data_frame_spark = retornar_data_frame_spark(get_session(), tabela_a_ser_migrada, dado_a_ser_migrado['separador'])

    df_log = save_log_dataframe(get_session(),
                                tabela_a_ser_migrada,
                                'TOTAL_A_MIGRAR', '',
                                data_frame_spark.count(),
                                sequence)

    save_dataframe_database(df_log, 'migracao_log', get_data_base_acess_airflow())

    estrutura_da_tabela_de_banco = get_dataframame_database(get_session(), tabela_a_ser_migrada,
                                                    get_data_base_acess_atd_araxa())

    data_frame_spark = seta_mascara_de_datas_no_dataframe(estrutura_da_tabela_de_banco, data_frame_spark)
    create_table_temp(data_frame_spark, tabela_a_ser_migrada)

    data_frame_tamanho_das_colunas = retorna_tamanho_das_colunas(get_session(), tabela_a_ser_migrada,
                                                                    get_data_base_acess_atd_araxa())

    filtro = retorna_filtro_registros_tamanho_correto(data_frame_tamanho_das_colunas)
    logger.warning(
        f"QUERIE PARA TAMANHO CORRETO:: {filtro}")

    filtro_excedido = retorna_filtro_registros_tamanho_excedido(data_frame_tamanho_das_colunas)
    if filtro_excedido is not None:
        data_frame_excedido = data_frame_spark.filter(filtro_excedido)
        data_frame_excedido.show()
        df_log = save_log_dataframe(get_session(), tabela_a_ser_migrada, 'TOTAL_ERROS_TAMANHO', '',
                                    data_frame_excedido.count(), sequence)
        save_dataframe_database(df_log, 'migracao_log', get_data_base_acess_airflow())

        ##asyncio.run(send_data_from_database(data_frame_excedido, tabela_a_ser_migrada, 'TOTAL_ERROS_TAMANHO', sequence))

    data_frame_valor_default = retorna_valor_default_colunas(get_session(), tabela_a_ser_migrada,
                                                                    get_data_base_acess_atd_araxa())

    data_frame_spark = seta_valor_default_das_colunas(data_frame_valor_default, data_frame_spark)
    logger.warning(
        f"DEPOIS DE SETAR OS VALORES DEFAULT:: {data_frame_spark.count()}")
    create_table_temp(data_frame_spark, tabela_a_ser_migrada)


    if filtro is not None:
        query = f"SELECT * FROM {tabela_a_ser_migrada} WHERE {filtro}"
        data_frame_spark = get_dataframe_from_query(get_session(), query)
        create_table_temp(data_frame_spark, tabela_a_ser_migrada)
        logger.warning(
            f"TOTAL QUERIE PARA TAMANHO CORRETO:: {data_frame_spark.count()}, {query}")

    create_table_temp(data_frame_spark, tabela_a_ser_migrada)

    if 'chave_primaria' in dado_a_ser_migrado:
        total_registros = data_frame_spark.count()
        registros_duplicados = retorna_registros_com_chave_duplicada(data_frame_spark,
                                                                     dado_a_ser_migrado['chave_primaria'])

        logger.warning(
            f"TOTAL_NO_DATA_FRAME {data_frame_spark.count()} TOTAL_DUPLICADOS ::  {registros_duplicados.count()}")

        data_frame_spark = remove_registros_com_chave_duplicada(data_frame_spark,
                                                                registros_duplicados, dado_a_ser_migrado['chave_primaria'])

        df_log = save_log_dataframe(get_session(), tabela_a_ser_migrada, 'TOTAL_CHAVE_PRIMARIA_DUPLICADA', '',
                                    abs(total_registros - registros_duplicados.count()), sequence)

        ##asyncio.run(send_data_from_database(registros_duplicados, tabela_a_ser_migrada, 'TOTAL_CHAVE_PRIMARIA_DUPLICADA', sequence))

        create_table_temp(data_frame_spark, tabela_a_ser_migrada)

        save_dataframe_database(df_log, 'migracao_log', get_data_base_acess_airflow())

    data_frame_constrains = get_constraints_type_check(get_session(), tabela_a_ser_migrada, get_data_base_acess_atd_araxa())

    if data_frame_constrains.count() > 0:
        for row in data_frame_constrains.collect():
            if 'constraints_a_nao_validar' in dado_a_ser_migrado:
                if row['CONSTRAINT_NAME'] in dado_a_ser_migrado['constraints_a_nao_validar']:
                    continue

            logger.warning(
                f"ANTES TOTAL_REGISTROS_ENCONTRADOS ANTES DA CONSTRAINT {row['CONSTRAINT_NAME']} ::  {data_frame_spark.count()}")
            data_frame_constraint_deferida = filtrar_registros_que_n_atendem_a_constraints(data_frame_spark, row)
            query = f"SELECT * FROM {tabela_a_ser_migrada} WHERE {adicionar_null_condition(ajustar_consulta_sql(row['SEARCH_CONDITION']))}"
            logger.warning(
                f"query {query}")

            data_frame_spark = get_dataframe_from_query(get_session(), query)
            create_table_temp(data_frame_spark, tabela_a_ser_migrada)

            logger.warning(
                f"TOTAL_REGISTROS_ENCONTRADOS DEPOIS DA CONSTRAINT {row['CONSTRAINT_NAME']} ::  {data_frame_spark.count()} e {row['SEARCH_CONDITION']}")

            if data_frame_constraint_deferida is not None:
                df_log = save_log_dataframe(get_session(), tabela_a_ser_migrada, f"TOTAL_ERROS_CONSTRAINTS_{row['CONSTRAINT_NAME']}", '',
                                            data_frame_constraint_deferida.count(), sequence)
                save_dataframe_database(df_log, 'migracao_log', get_data_base_acess_airflow())


    if 'tabelas_join' in dado_a_ser_migrado:
        for tabela in dado_a_ser_migrado['tabelas_join']:
            df_tabela_join = get_dataframame_database(get_session(), tabela['nome_tabela'], get_data_base_acess_atd_araxa())
            create_table_temp(df_tabela_join, tabela['nome_tabela'])


    if 'tabelas_join' in dado_a_ser_migrado:
        for tabela_join in dado_a_ser_migrado['tabelas_join']:
            logger.warning(
                f"ANTES TOTAL_REGISTROS_ENCONTRADOS:: {data_frame_spark.count()}")
            query = monta_join_com_tabela(tabela_a_ser_migrada,
                                          tabela_join['nome_tabela'],
                                          tabela_join['colunas_da_principal'],
                                          tabela_join['colunas_do_tabela_join'])

            query_not_found = monta_registro_nao_encontrado(tabela_a_ser_migrada,
                                          tabela_join['nome_tabela'],
                                          tabela_join['colunas_da_principal'],
                                          tabela_join['colunas_do_tabela_join'])

            data_frame_spark = get_dataframe_from_query(get_session(), query)
            logger.warning(f"TOTAL_REGISTROS_ENCONTRADOS:: {data_frame_spark.count()} {tabela_join['nome_tabela']} {query}")

            data_frame_not_found = get_dataframe_from_query(get_session(), query_not_found)

            df_log = save_log_dataframe(get_session(), tabela_a_ser_migrada, f"TOTAL_ERROS_JOIN_{tabela_join['nome_tabela']}", '',
                                        data_frame_not_found.count(), sequence)
            save_dataframe_database(df_log, 'migracao_log', get_data_base_acess_airflow())

            create_table_temp(data_frame_spark, tabela_a_ser_migrada)

    try:
        save_dataframe_database(data_frame_spark, tabela_a_ser_migrada, get_data_base_acess_atd_araxa())
        df_log = save_log_dataframe(get_session(), tabela_a_ser_migrada,
                                    "TOTAL_REGISTROS_MIGRADOS", '',
                                    data_frame_spark.count(), sequence)

        save_dataframe_database(df_log, 'migracao_log', get_data_base_acess_airflow())
    except Exception as e:
        logger.warning(
            f"OCORREU UM ERRO AO SALVAR OS DADOS {e} FIM")

        df_log = save_log_dataframe(get_session(), tabela_a_ser_migrada,
                                    "TOTAL_REGISTROS_EXCEPTION", '',
                                    data_frame_spark.count(), sequence)
        save_dataframe_database(df_log, 'migracao_log', get_data_base_acess_airflow())








