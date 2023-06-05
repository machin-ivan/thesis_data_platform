-- This script provides data migration from stg layer to dds layer

INSERT INTO dds.stable_symbols (symbol)
SELECT DISTINCT symbol
FROM stg.pools
ON CONFLICT DO NOTHING;

INSERT INTO dds.chains ("chain")
SELECT DISTINCT "chain"
FROM stg.pools p
UNION 
SELECT DISTINCT "chain"
FROM stg.reward_tokens rt
ON CONFLICT DO NOTHING;

INSERT INTO dds.projects (project_name)
SELECT DISTINCT project
FROM stg.pools
ON CONFLICT DO NOTHING;

INSERT INTO dds.reward_tokens (symbol, "chain", contract_addr, tvl, 
	mcap_to_tvl, fdv_to_tvl, mcap, market_cap_rank, fdv, 
	price_change_24h, price_change_7d, price_change_30d, 
	price_change_60d, price_change_200d)
SELECT symbol, dc.id, contract_addr, tvl, mcap_to_tvl, fdv_to_tvl,
	mcap, market_cap_rank, fdv, price_change_24h, price_change_7d,
	price_change_30d, price_change_60d, price_change_200d
FROM stg.reward_tokens rt 
JOIN dds.chains dc ON rt."chain" = dc."chain";

INSERT INTO dds.pools (pool_id, "date_", chain_id, project_id, symbol_id, tvlusd,
	apybase, apyreward, apy)
SELECT pool_id, ts::date, c.id, pr.id, ss.id, tvlusd, apybase,
	apyreward, apy
FROM stg.pools p 
JOIN dds.chains c ON p."chain" = c."chain"
JOIN dds.projects pr ON p.project = pr.project_name 
JOIN dds.stable_symbols ss ON p.symbol = ss.symbol;

INSERT INTO dds.pool_history (pool_id, "date_", tvlusd, apy, 
apybase, apyreward)
SELECT p.id, ph.ts::date, ph.tvlusd, ph.apy, ph.apybase, ph.apyreward
FROM stg.pool_history ph 
JOIN dds.pools p ON ph.pool_id = p.pool_id;


UPDATE dds.pools SET
	apyreward  = apy,
	apybase = 0
WHERE (apybase = -1 AND apyreward = -1);

UPDATE dds.pool_history SET
	apyreward  = apy,
	apybase = 0
WHERE (apybase = -1 AND apyreward = -1);

UPDATE dds.pools SET
	apybase = 0
WHERE apybase = -1;

UPDATE dds.pools SET
	apyreward = 0
WHERE apyreward = -1;

UPDATE dds.pool_history SET
	apybase = 0
WHERE apybase = -1;

UPDATE dds.pool_history SET
	apyreward = 0
WHERE apyreward = -1;