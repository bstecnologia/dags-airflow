import sys
import json
from helper.spark_helper import get_session ,retornar_data_frame_spark
import socket

if __name__ == '__main__':
    host_name = socket.gethostname()
    # Obtém o endereço IP associado ao nome do host
    ip_address = socket.gethostbyname(host_name)
    file_for_migration = sys.argv[1]
    dado_a_ser_migrado = json.loads(file_for_migration)

    tabela_a_ser_migrada = dado_a_ser_migrado['tabela_a_ser_migrada']

    data_frame_spark = retornar_data_frame_spark(get_session(), tabela_a_ser_migrada, dado_a_ser_migrado['separador'])

    data_frame_spark.show()








