def get_data_base_acess_atd():
    return {
        'url': 'jdbc:oracle:thin:@10.0.0.169:1521:ee',
        'properties': {
            'user': 'ATD',
            'password': 'password',
            'driver': 'oracle.jdbc.driver.OracleDriver'
        }
    }

def get_data_base_acess_atd_araxa():
    return {
        'url': 'jdbc:oracle:thin:@//sgu164ho.oracle.sgusuite.com.br:1521/sgu164ho',
        'properties': {
            'user': 'UD999',
            'password': 'hygS46gs37s',
            'driver': 'oracle.jdbc.driver.OracleDriver'
        }
    }


def get_data_base_acess_integracao_replica():
    return {
        'url': 'jdbc:oracle:thin:@//czitr02h.oracle.sgusuite.com.br:1521/wspdev1',
        'properties': {
            'user': 'INTEGRACAO_REPLICA',
            'password': 'integracao_ztrs',
            'driver': 'oracle.jdbc.driver.OracleDriver'
        }
    }

def get_data_base_acess_airflow():
    return {
        'url': 'jdbc:postgresql://postgres:5432/airflow',
        'properties': {
            'user': 'airflow',
            'password': 'airflow',
            'driver': 'org.postgresql.Driver'
        }
    }

def get_data_base_replica_sql_server():
    return {
        'url': 'jdbc:sqlserver://10.0.0.169:1433;databaseName=MIGRACAO_REPLICA',
        'properties': {
            'user': 'sa',
            'password': 'SqlServer2019!',
            'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            'encrypt': 'true',
            'trustServerCertificate': 'true',
        }
    }