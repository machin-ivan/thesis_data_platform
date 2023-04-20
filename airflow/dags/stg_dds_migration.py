import psycopg2
import re
import os

import config


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


def load_pools_rewtokens_rel_table(conn_params: dict):
    """
    Extracts (pool_id, rew_token_address) unique pairs and loads
    it into pools_rewtokens_rel table

    Args:
        conn_params::dict
    """
    res = []
    sql = """
        SELECT pool_id, rewardtokens
        FROM stg.pools
    """
    lines = get_from_db(sql=sql, conn_params=conn_params)
    for line in lines:
        pid, rtl = line[0], line[1]
        if rtl != 'None' and rtl != '[]':
            rew_tokens = (re.sub('[\[\]" ]', '', rtl)).split(',')
            res += [(pid, rt) for rt in rew_tokens]

    res = list(set(res))
    s = re.sub('[\[\]]', '', str(res))
    sql = f"INSERT INTO dds.pools_rewtokens_rel VALUES {s}"

    run_sql_script(sql=sql, conn_params=config.conn_params)



def run_stg_dds_mirgation():
    # Load and run sql script to load all the tables in dds layer
    # except one
    cur_dir = os.path.dirname(__file__)
    rel_path = "/sql/stg_dds_migration.sql"
    path = cur_dir + rel_path

    with open(path, 'r') as f:
        sql = f.read()
    
    run_sql_script(sql=sql, conn_params=config.conn_params)

    # Load last one table "pools_rewtokens_rel"
    load_pools_rewtokens_rel_table(conn_params=config.conn_params)
