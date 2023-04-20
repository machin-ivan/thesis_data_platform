import config
import datetime
import time
import requests
import psycopg2


def filter_pools(pools_list: list):
    """
    Filters list of dicts of pools in-place, leaves only those that 
    are satisfiyng conditions

    Args:
        pools_list::dict
    """
    pools_list[:] = [d for d in pools_list if d['symbol'] in ['USDT', 'USDC', 'DAI']
                                    and d['tvlUsd'] > 1e6
                                    and float(d['apy']) != 0]


def pools_transform_response(pools_list: list):
    """
    Takes a list of dicts with pools info and transforms it to a list of strings
    formatted to load in db

    Args:
        pools_list::list
    Returns:
        vals::list
            List of strings with values for sql script

    """
    vals_to_insert = []
    cur_ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for line in pools_list:
        tmp = f"'{cur_ts}', '{line['chain']}', '{line['project']}', " \
            f"'{line['symbol']}', {line['tvlUsd']}, " \
            f"{line['apyBase'] if line['apyBase'] is not None else 0}, "\
            f"{line['apyReward'] if line['apyReward'] is not None else 0}, "\
            f"""{line['apy']},'{str(line['rewardTokens']).replace("'", '"')}', """ \
            f"'{line['pool']}'"
        vals_to_insert.append(tmp)
    
    return vals_to_insert


def pools_hist_transform_response(data: list,
                                  id: str):
    """
    Takes a list of history data of pool and transforms it to a list of strings
    formatted to load in db

    Args:
        data::list
            List of dicts with historical data
        id::str
            Id of the pool
    Returns:
        vals::list
            List of strings with values for sql script
    """
    vals_to_insert = []

    for line in data:
        tmp = f"'{id}', '{line['timestamp']}', {line['tvlUsd']}, " \
        f"{line['apy']}, " \
        f"{line['apyBase'] if line['apyBase'] is not None else 0}, " \
        f"{line['apyReward'] if line['apyReward'] is not None else 0} "

        vals_to_insert.append(tmp)
    
    return vals_to_insert


def line_from_reward_token_response(data: dict,
                                  address: str,
                                  chain: str):
    """
    Returns list of strings for token_history table formatted to load in db

    Args:
        data::dict
            Token data
        address::str
            Address of token contract
        chain::str
            Name of chain
    Returns:
        vals::list
            List of strings with values for sql script
    """
    md = data['market_data']
    
    tmp = f"'{data['symbol']}', '{chain}', '{address}', " \
    f"{md['total_value_locked']['usd'] if md['total_value_locked'] is not None else 0}, " \
    f"{md['mcap_to_tvl_ratio'] if md['mcap_to_tvl_ratio'] is not None else 0}, " \
    f"{md['fdv_to_tvl_ratio'] if md['fdv_to_tvl_ratio'] is not None else 0}, " \
    f"{md['market_cap']['usd'] if md['market_cap']['usd'] is not None else 0}, " \
    f"{md['market_cap_rank'] if md['market_cap_rank'] is not None else 0}, " \
    f"{md['fully_diluted_valuation']['usd'] if md['fully_diluted_valuation'] != {} else 0}, " \
    f"{md['price_change_percentage_24h']}, {md['price_change_percentage_7d']}, {md['price_change_percentage_30d']}, " \
    f"{md['price_change_percentage_60d']}, {md['price_change_percentage_200d']}"
    
    return [tmp]
    

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


def get_and_load_pools_hist(pool_ids: list):
    """
    Takes a list of history data of pools and loads it in stg.pool_history
    table in db

    Args:
        pool_ids::list
            List of pools_ids
    """
    for id in pool_ids:
        url = 'https://yields.llama.fi'
        endpoint = f'/chart/{id}'

        response = requests.get(url + endpoint).json()
        assert response['status'] == 'success'

        data = response['data']
        lines = pools_hist_transform_response(data=data, id=id)
        load_to_db(conn_params=config.conn_params,
            table_name='stg.pool_history',
            columns='(pool_id, ts, tvlusd, apy, apybase, apyreward)',
            lines=lines)


def extract_reward_tokens(pools_list: list):
    """
    Takes a list of pools data, extracts reward tokens addresses
    and returns list of unique (chain, address) tuples

    Args:
        pools_list::list
    Returns:
        reward_tokens::list
            List of unqiue (chain, address) tuples for every reward token
    """
    reward_tokens = set()

    for pool in pools_list:
        if pool['rewardTokens'] is None:
            continue
        chain, tokens = pool['chain'], list(pool['rewardTokens'])
        for token in tokens:
            reward_tokens.add((chain, token))
    return list(reward_tokens)


def get_asset_platforms():
    while True:
        tmp = requests.get('https://api.coingecko.com/api/v3/asset_platforms').json()
        if (type(tmp) is dict) and (tmp.get('status', 0)):
            time.sleep(5)
            continue
        break

    asset_platforms = dict()
    for d in tmp:
        if d['id'] == '':
            continue
        asset_platforms[d['name']] = d['id']
    # exceptions
    asset_platforms['Arbitrum'] = 'arbitrum-one'
    asset_platforms['Polygon'] = 'polygon-pos'
    asset_platforms['BSC'] = 'binance-smart-chain'
    asset_platforms['Tron'] = 'tron'
    return asset_platforms


def get_and_load_reward_tokens(reward_tokens: list,
                            asset_platforms: dict):
    """
    Takes a list of reward_tokens and gets the historical data for every one
    and loads it into db in table token_hsitory

    Args:
        reward_tokens::list
            List of reward tokens
    """
    for token in reward_tokens:
        chain_name, address = token[0], token[1]
        
        while True:
            url = f'https://api.coingecko.com/api/v3/coins/' \
                f'{asset_platforms.get(chain_name, "")}/contract/{address}'
            response = requests.get(url).json()
            if response.get('error', 1) != 1:
                break
            if (response is None) or response.get('status', 0):
                time.sleep(5)
                continue

            line = line_from_reward_token_response(data=response,
                                                address=address,
                                                chain=chain_name)
            
            load_to_db(conn_params=config.conn_params,
                table_name='stg.reward_tokens',
                columns='(symbol, "chain", contract_addr, tvl, mcap_to_tvl, ' \
                'fdv_to_tvl, mcap, market_cap_rank, fdv, price_change_24h, ' \
                'price_change_7d, price_change_30d, price_change_60d, price_change_200d)',
                lines=line)
            break


def run_stg_load_script():
    # load pools info (stg.pools table)
    url = 'https://yields.llama.fi'
    endpoint = '/pools'

    response = requests.get(url + endpoint).json()
    assert response['status'] == 'success'

    pools_list = response['data']
    filter_pools(pools_list)

    # load pools info into db (stg.pool_history table)
    load_to_db(conn_params=config.conn_params,
            table_name='stg.pools',
            columns='(ts, chain, project, symbol, tvlusd, apybase, apyreward, apy, rewardtokens, pool_id)',
            lines=pools_transform_response(pools_list))

    pool_ids = [d['pool'] for d in pools_list]
    get_and_load_pools_hist(pool_ids)

    # load reward_tokens table
    reward_tokens = extract_reward_tokens(pools_list)
    asset_platforms = get_asset_platforms()
    get_and_load_reward_tokens(reward_tokens, asset_platforms)

