import pandas as pd
import os

current_dir = os.path.dirname(os.path.abspath('airflow'))

def get_pandas_df(file_name, sepatator):
    arquivos_dir = os.path.join(current_dir,'dags','migracao_csv','arquivos')
    pandas_df = pd.read_csv(os.path.join(arquivos_dir, file_name), sep=sepatator, dtype=str)
    pandas_df = pandas_df.fillna('')
    pandas_df = pandas_df.where(pd.notnull(pandas_df), None)
    return pandas_df

def get_pandas_dataframe(file_name, sepatator):
    arquivos_dir = os.path.join(current_dir,'dags','migracao_csv','arquivos')
    pandas_df = pd.read_csv(os.path.join(arquivos_dir, f"{file_name}.csv" ), sep=sepatator, dtype=str)
    pandas_df = pandas_df.fillna('')
    pandas_df = pandas_df.where(pd.notnull(pandas_df), None)
    return pandas_df

def get_pandas_dataframe_2(session,file_name, sepatator):
    arquivos_dir = os.path.join(current_dir,'dags','migracao_csv','arquivos')
    pandas_df = pd.read_csv(os.path.join(arquivos_dir, f"{file_name}.csv" ), sep=sepatator, dtype=str)
    pandas_df = pandas_df.fillna('')
    df = session.read.load(os.path.join(arquivos_dir, f"/opt/spark-data/{file_name}.csv"),format = "csv", inferSchema="true", sep=sepatator, header="true"
      )
    df.show()
    pandas_df = pandas_df.where(pd.notnull(pandas_df), None)
    return pandas_df