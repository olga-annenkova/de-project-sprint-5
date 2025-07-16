--STG
--курьеры GET /couriers
create table stg.stg_couriers (
	id serial primary key NOT NULL,
    object_id varchar NOT NULL,
	object_value text NOT NULL,
	loaded_at timestamp NOT NULL -- дата и время загрузки в STG
);
ALTER TABLE stg.stg_couriers
ADD CONSTRAINT stg_couriers_object_id_key UNIQUE (object_id);

--доставка GET / deliveries
create table stg.stg_deliveries (
	id serial primary key NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	loaded_at timestamp NOT NULL -- дата и время загрузки в STG
);
ALTER TABLE stg.stg_deliveries
ADD CONSTRAINT stg_deliveries_object_id_key UNIQUE (object_id);

create table stg.stg_restaurants (
	id serial primary key NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	loaded_at timestamp NOT NULL -- дата и время загрузки в STG
);
ALTER TABLE stg.stg_restaurants
ADD CONSTRAINT stg_restaurants_object_id_key UNIQUE (object_id);
