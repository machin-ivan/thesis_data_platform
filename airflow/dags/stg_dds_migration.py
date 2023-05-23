import re
import os

import config
from sql_helper_functions import run_sql_script, get_from_db


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
