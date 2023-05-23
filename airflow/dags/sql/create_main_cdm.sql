create materialized view if not exists cdm.main_datamart as
with cte as (
	select ph.pool_id,
	ph."date_",
	LAG(tvlusd, 7) over (partition by ph.pool_id order by ph."date_") tvllag7d,
	LAG(apy, 7) over (partition by ph.pool_id order by ph."date_") apylag7d,
	LAG(tvlusd, 30) over (partition by ph.pool_id order by ph."date_") tvllag30d,
	LAG(apy, 30) over (partition by ph.pool_id order by ph."date_") apylag30d,
	LAG(tvlusd, 90) over (partition by ph.pool_id order by ph."date_") tvllag90d,
	LAG(apy, 90) over (partition by ph.pool_id order by ph."date_") apylag90d
from dds.pool_history ph
), cte2 as (
	select prr.pool_id,
		coalesce(AVG(t.rew_token_coef), 0) weight_coef
	from dds.pools_rewtokens_rel prr
	left join (select * from dds.reward_tokens rt
				join dds.reward_tokens_clusters rtc
				on rt.id = rtc.rew_token_id) t
	on prr.rew_token_addr = t.contract_addr
	group by prr.pool_id
)
select p.pool_id,
	pr.project_name,
	s.symbol,
	c."chain",
	p.tvlusd,
	p.apy,
	p.apybase,
	p.apyreward,
	cte2.weight_coef apyreward_coef,
	p.apyreward * cte2.weight_coef apyreward_weighted,
	p.apybase + p.apyreward * cte2.weight_coef apy_weighted,
	p."date_",
	tvllag7d,
	apylag7d,
	tvllag30d,
	apylag30d,
	tvllag90d,
	apylag90d
FROM cte
join dds.pools p ON p.id = cte.pool_id
JOIN dds.chains c ON p.chain_id = c.id
JOIN dds.projects pr ON p.project_id = pr.id 
JOIN dds.stable_symbols s ON p.symbol_id = s.id
join cte2 on p.pool_id = cte2.pool_id
WHERE cte."date_" = current_date - 1

