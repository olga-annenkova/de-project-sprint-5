--DDS
--справочник курьеров
CREATE TABLE IF NOT EXISTS dds.dm_couriers (
    id serial primary key not null,
    courier_id varchar not null,
    courier_name varchar not null
);
ALTER TABLE dds.dm_couriers
ADD CONSTRAINT dm_couriers_courier_id_key UNIQUE (courier_id);

--справочник ресторанов 
CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
    id serial primary key not null,
    restaurant_id varchar not null,
    restaurant_name varchar not null
);
ALTER TABLE dds.dm_restaurants
  ADD CONSTRAINT dm_restaurants_restaurant_id_key UNIQUE (restaurant_id);

--таблица доставок (факт)
CREATE TABLE IF NOT EXISTS dds.dm_deliveries (
    id serial primary key not null,
    order_id varchar not null,
    order_ts timestamp not null,
    delivery_id varchar not null,
    courier_id varchar not null,
    "address" varchar not null,
    delivery_ts timestamp not null,
    rate integer not null,
    tip_sum numeric(14,2) not null,
    sum numeric(14,2) not null
);
ALTER TABLE dds.dm_deliveries
  ADD CONSTRAINT dm_deliveries_delivery_id_key UNIQUE (delivery_id);



