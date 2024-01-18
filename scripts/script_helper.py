import os

current_dir = os.path.dirname(os.path.abspath(__file__))


def get_sql_queries(filename):
    with open(os.path.join(current_dir, filename), 'r') as file:
        sql_queries = file.read()
    return sql_queries
