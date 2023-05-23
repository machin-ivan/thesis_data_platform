-- This script sets up the structure of Data Warehouse
-- Run it first after setting up PostgreSQL server


-- Creating schemas of layers

CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS cdm;

-- Creating Staging layer tables

CREATE TABLE IF NOT EXISTS stg.pools (
	id serial4 NOT NULL,
	ts timestamp NOT NULL,
	"chain" varchar(30) NOT NULL,
	project varchar(50) NOT NULL,
	symbol varchar(5) NOT NULL,
	tvlusd int8 NOT NULL,
	apybase numeric(8, 5) NOT NULL,
	apyreward numeric(8, 5) NOT NULL,
	apy numeric(8, 5) NOT NULL,
	rewardtokens text NOT NULL,
	pool_id varchar(36) NOT NULL,
	CONSTRAINT pools_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS stg.pool_history (
	id serial4 NOT NULL,
	pool_id varchar(36) NOT NULL,
	ts timestamp NOT NULL,
	tvlusd int8 NOT NULL,
	apy numeric(8, 5) NOT NULL,
	apybase numeric(8, 5) NOT NULL,
	apyreward numeric(8, 5) NOT NULL,
	CONSTRAINT pool_history_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS stg.reward_tokens (
	id serial4 NOT NULL,
	symbol varchar(10) NOT NULL,
	"chain" varchar(30) NOT NULL,
	contract_addr text NOT NULL,
	tvl int8 NOT NULL,
	mcap_to_tvl float4 NOT NULL,
	fdv_to_tvl float4 NOT NULL,
	mcap int8 NOT NULL,
	market_cap_rank int2 NOT NULL,
	fdv int8 NOT NULL,
	price_change_24h float4 NOT NULL,
	price_change_7d float4 NOT NULL,
	price_change_30d float4 NOT NULL,
	price_change_60d float4 NOT NULL,
	price_change_200d float4 NOT NULL,
	CONSTRAINT reward_tokens_pk PRIMARY KEY (id)
);

-- Creating DDS schema

CREATE TABLE IF NOT EXISTS dds.stable_symbols (
	id serial4 NOT NULL,
	symbol varchar(4) NOT NULL,
	CONSTRAINT stable_symbols_un UNIQUE (symbol),
	CONSTRAINT symbols_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS dds.chains (
	id serial4 NOT NULL,
	"chain" varchar(30) NOT NULL,
	CONSTRAINT chains_pk PRIMARY KEY (id),
	CONSTRAINT chains_un UNIQUE (chain)
);

CREATE TABLE IF NOT EXISTS dds.projects (
	id serial4 NOT NULL,
	project_name varchar(40) NOT NULL,
	CONSTRAINT projects_pk PRIMARY KEY (id),
	CONSTRAINT projects_un UNIQUE (project_name)
);

CREATE TABLE IF NOT EXISTS dds.pools (
	id serial4 NOT NULL,
	pool_id varchar(36) NOT NULL,
	date_ date NOT NULL,
	chain_id int4 NOT NULL,
	project_id int4 NOT NULL,
	symbol_id int4 NOT NULL,
	tvlusd int8 NOT NULL,
	apybase numeric(8, 5) NOT NULL,
	apyreward numeric(8, 5) NOT NULL,
	apy numeric(8, 5) NOT NULL,
	CONSTRAINT pools_pk PRIMARY KEY (id),
	CONSTRAINT pools_fk FOREIGN KEY (chain_id) REFERENCES dds.chains(id),
	CONSTRAINT pools_fk_1 FOREIGN KEY (project_id) REFERENCES dds.projects(id),
	CONSTRAINT pools_fk_2 FOREIGN KEY (symbol_id) REFERENCES dds.stable_symbols(id)
);

CREATE TABLE IF NOT EXISTS dds.pool_history (
	id serial4 NOT NULL,
	pool_id int4 NOT NULL,
	date_ date NOT NULL,
	tvlusd int8 NOT NULL,
	apy numeric(8, 5) NOT NULL,
	apybase numeric(8, 5) NOT NULL,
	apyreward numeric(8, 5) NOT NULL,
	CONSTRAINT pool_history_pk PRIMARY KEY (id),
	CONSTRAINT pool_history_fk FOREIGN KEY (pool_id) REFERENCES dds.pools(id)
);

CREATE TABLE IF NOT EXISTS dds.reward_tokens (
	id serial4 NOT NULL,
	symbol varchar(10) NOT NULL,
	"chain" int4 NOT NULL,
	contract_addr text NOT NULL,
	tvl int8 NOT NULL,
	mcap_to_tvl float4 NOT NULL,
	fdv_to_tvl float4 NOT NULL,
	mcap int8 NOT NULL,
	market_cap_rank int2 NOT NULL,
	fdv int8 NOT NULL,
	price_change_24h float4 NOT NULL,
	price_change_7d float4 NOT NULL,
	price_change_30d float4 NOT NULL,
	price_change_60d float4 NOT NULL,
	price_change_200d float4 NOT NULL,
	CONSTRAINT reward_tokens_pk PRIMARY KEY (id),
	CONSTRAINT reward_tokens_fk FOREIGN KEY ("chain") REFERENCES dds.chains(id)
);

CREATE TABLE IF NOT EXISTS dds.pools_rewtokens_rel (
	pool_id varchar NOT NULL,
	rew_token_addr text NOT NULL,
	CONSTRAINT pools_rewtokens_rel_pk PRIMARY KEY (pool_id,rew_token_addr)
);


CREATE TABLE IF NOT EXISTS dds.reward_tokens_clusters (
	id serial4 NOT NULL,
	rew_token_id int8 NOT NULL,
	stability_24h int2 NOT NULL,
	stability_7d int2 NOT NULL,
	stability_30d int2 NOT NULL,
	stability_60d int2 NOT NULL,
	stability_200d int2 NOT NULL,
	mcap_rating int2 NOT NULL,
	rew_token_coef float4 NOT NULL,
	CONSTRAINT reward_tokens_clusters_fk FOREIGN KEY (rew_token_id) REFERENCES dds.reward_tokens(id)
);