import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

import config
from sql_helper_functions import load_to_db, get_from_db


def calculate_stability_ratings(df: pd.DataFrame,
                               num_clusters=5,
                               random_state=0):
    """
    Clusterizing every token using KMeans by price change and assigning the stability rating
    in range (1 to 5) where 5 is the most stable ones for every time period
    
    Args:
        df::pd.DataFrame
        num_clusters::int
        random_state::int
    """
    price_change_columns = ['price_change_24h', 'price_change_7d', 'price_change_30d', 
                            'price_change_60d', 'price_change_200d']

    for column in price_change_columns:
        # Extract the column data and take the absolute values
        column_data = df[column].abs().values.reshape(-1, 1)
        
        kmeans = KMeans(n_clusters=num_clusters, random_state=random_state)
        kmeans.fit(column_data)
        
        tmp = column.split('_')
        df[tmp[2] + '_stability'] = kmeans.labels_ + 1
        
        # Sort the clusters based on the mean absolute values
        cluster_means = df.groupby(tmp[2] + '_stability')[column].mean().abs()
        sorted_clusters = cluster_means.sort_values(ascending=False).index
        df[tmp[2] + '_stability'] = df[tmp[2] + '_stability'].map({k: v for v, k in enumerate(sorted_clusters, 1)})


def calculate_mcap_clusters(df: pd.DataFrame,
                            num_clusters=5,
                            random_state=0):
    """
    Clusterizing every token by market cap using KMeans first performing logarithmic 
    transformation on values and assigning the rating in range (1 to 5)
    where 5 is the most stable ones for every time period
    
    Args:
        df::pd.DataFrame
        num_clusters::int
        random_state::int
    """
    # Extract the market cap data and applying logarithmic transformation
    log_market_cap_data = np.log(df['mcap'] + 1).values.reshape(-1, 1)

    kmeans = KMeans(n_clusters=num_clusters, random_state=random_state)
    kmeans.fit(log_market_cap_data)

    df['mcap_rating'] = kmeans.labels_

    # Sort the clusters based on the mean absolute values
    cluster_means = df.groupby('mcap_rating')['mcap'].mean()
    sorted_clusters = cluster_means.sort_values().index
    df['mcap_rating'] = df['mcap_rating'].map({k: v + 1 for v, k in enumerate(sorted_clusters)})


def calculate_ratings(df: pd.DataFrame):
    """
    """
    stabilities_cols = ['24h_stability', '7d_stability', '30d_stability', '60d_stability', '200d_stability']

    # Defining and normalizing the weights
    stability_weights = np.array([1, 2, 3, 4, 5])
    stability_weights = stability_weights / np.sum(stability_weights)

    df_tmp = df.copy()
    df_tmp['stability_rating'] = np.dot(df_tmp[stabilities_cols], stability_weights)

    # Rescale the ratings from (1,5) range to (0, 1)
    df_tmp['stability_rating'] = (df_tmp['stability_rating'] - 1) / (5 - 1)

    # Normalize the mcap_cluster rating to a range from 0 to 1
    df_tmp['mcap_rating'] = (df_tmp['mcap_rating'] - 1) / (5 - 1)

    # Calculate the coefficients as mean between mcap and stability ratings
    df_tmp['rew_token_coef'] = 0.5 * df_tmp['mcap_rating'] + 0.5 * df_tmp['stability_rating']
    df['rew_token_coef'] = df_tmp['rew_token_coef']


def rt_clusterize():
    """
    Load reward tokens data from db, clusterize it, calculate final coefficients
    """
    conn_params = config.conn_params
    sql = 'select id, symbol, chain, contract_addr, tvl, mcap_to_tvl, fdv_to_tvl, mcap, ' \
        'market_cap_rank, fdv, price_change_24h, price_change_7d, price_change_30d, ' \
        'price_change_60d, price_change_200d from dds.reward_tokens'
    lines = get_from_db(sql=sql, conn_params=conn_params)

    df = pd.DataFrame(lines)
    names=['id', 'symbol', 'chain', 'contract_addr', 'tvl', 'mcap_to_tvl', 
       'fdv_to_tvl', 'mcap', 'market_cap_rank', 'fdv', 'price_change_24h',
       'price_change_7d', 'price_change_30d', 'price_change_60d', 'price_change_200d']
    df.columns = names

    calculate_stability_ratings(df)
    calculate_mcap_clusters(df)
    calculate_ratings(df)

    # Load results in db
    res_cols_str = '(rew_token_id, stability_24h, stability_7d, stability_30d, stability_60d, stability_200d, mcap_rating, rew_token_coef, "date_")'
    res_cols = ['id', '24h_stability', '7d_stability', '30d_stability', '60d_stability', '200d_stability', 'mcap_rating', 'rew_token_coef']
    res = list(df[res_cols].to_records(index=False))
    lines = [", ".join(map(str, list(i))) for i in res]
    lines = [line + ', current_date' for line in lines]

    load_to_db(conn_params=config.conn_params,
               lines=lines, 
               table_name='dds.reward_tokens_clusters', 
               columns=res_cols_str)