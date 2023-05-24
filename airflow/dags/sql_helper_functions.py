import psycopg2
import os

import config


def load_to_db(conn_params:dict,
               table_name: str,
               columns: str,
               lines: list):
    """
    Creates connection with db and loads lines one by one

    Args:
        conn_params::dict
        table_name::str
        columns::str
        lines::list
    """
    conn = psycopg2.connect(host="postgres",
                        database=conn_params['database'],
                        port=conn_params['port'],
                        user=conn_params['user'],
                        password=conn_params['password'])

    cursor = conn.cursor()
    conn.autocommit = True

    for line in lines:
        sql = f"INSERT INTO {table_name} {columns} VALUES ({line}); \n"
        cursor.execute(sql)

    cursor.close()
    conn.close()


def run_sql_script(sql: str,
                   conn_params:dict):
    """
    Connects to db and runs the sql script

    Args:
        sql::str
            SQL script
    """
    conn = psycopg2.connect(host=conn_params['host'],
                            database=conn_params['database'],
                            port=conn_params['port'],
                            user=conn_params['user'],
                            password=conn_params['password'])
    cursor = conn.cursor()
    conn.autocommit = True

    cursor.execute(sql)

    cursor.close()
    conn.close()


def get_from_db(sql: str,
                conn_params: dict):
    """
    Performs SELECT operation to a db and returns its result

    Returns:
        lines::list
            Result of SELECT operation
    """
    conn = psycopg2.connect(host=conn_params['host'],
                            database=conn_params['database'],
                            port=conn_params['port'],
                            user=conn_params['user'],
                            password=conn_params['password'])
    cursor = conn.cursor()
    conn.autocommit = True

    cursor.execute(sql)
    lines = cursor.fetchall()

    cursor.close()
    conn.close()
    return lines


def clear_stg_func():
    """
    Run sql script to clear staging layer by truncating all tables
    """

    sql = '''
        TRUNCATE stg.pool_history;
        TRUNCATE stg.pools;
        TRUNCATE stg.reward_tokens;
    '''
    
    run_sql_script(sql=sql, conn_params=config.conn_params)


def reset_dds_func():
    """
    Run sql script to clear DDS layer by truncating all tables
    """

    sql = '''
        TRUNCATE dds.pools_rewtokens_rel CASCADE;
        TRUNCATE dds.pool_history CASCADE;
        TRUNCATE dds.reward_tokens CASCADE;
        TRUNCATE dds.pools CASCADE;
        TRUNCATE dds.reward_tokens_clusters CASCADE;
    '''
    
    run_sql_script(sql=sql, conn_params=config.conn_params)


def main_datamart_create():
    cur_dir = os.path.dirname(__file__)
    rel_path = "/sql/create_cdm.sql"
    path = cur_dir + rel_path

    with open(path, 'r') as f:
        sql = f.read()
    
    run_sql_script(sql=sql, conn_params=config.conn_params)
