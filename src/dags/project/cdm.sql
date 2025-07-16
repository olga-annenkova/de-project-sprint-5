-- витрина расчётов с курьерами
CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
	id serial NOT NULL,
	courier_id varchar NOT NULL, 
	courier_name varchar NOT NULL, 
	settlement_year integer NOT NULL, 
    settlement_month integer NOT NULL,
	orders_count integer NOT NULL, 
	orders_total_sum numeric(14,2) NOT NULL, 
	rate_avg numeric(14,2) NOT NULL,
	order_processing_fee numeric(14,2) NOT NULL,
	courier_order_sum numeric(14,2) NOT NULL,
	courier_tips_sum numeric(14,2) NOT NULL,
	courier_reward_sum numeric(14,2) NOT NULL
);

ALTER TABLE cdm.dm_courier_ledger
ADD CONSTRAINT dm_courier_ledger_pkey PRIMARY KEY (id);

ALTER TABLE cdm.dm_courier_ledger
ADD CONSTRAINT dm_courier_ledger_unique UNIQUE (courier_id, settlement_year, settlement_month);


