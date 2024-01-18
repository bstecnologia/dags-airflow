import logging
import requests
import json

def logger_error(message):
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.ERROR)
    logging.error(message)

def logger_info(message):
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    logging.info(message)

def logger_debug(message):
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.debug(message)

def logger_warning(message):
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.WARNING)
    logging.warning(message)

def send_logger_from_database(data_frame, tabela_a_ser_migrada, tipo_log):
    url = 'http://10.0.0.169:9096/log'
    ##data_json = data_frame.map(lambda x: json.dumps(x))
    print(data_frame)
    response = requests.post(url, json={"key":tabela_a_ser_migrada,
                                        "data": {"tabela_a_ser_migrada": tabela_a_ser_migrada,
                                                 "tipo_log": tipo_log,
                                                 "dados": json.loads(data_frame)}})
    print(response.status_code)
