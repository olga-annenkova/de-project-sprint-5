
1. Список полей, необходимых для витрины выплат курьерам
id	Идентификатор записи в витрине
courier_id	ID курьера
courier_name	Ф.И.О. курьера
settlement_year	Год расчётного периода
settlement_month	Месяц расчётного периода (1–12)
orders_count	Количество заказов за месяц
orders_total_sum	Общая сумма заказов
rate_avg	Средний рейтинг курьера за месяц
order_processing_fee	Сумма удержания компании (orders_total_sum * 0.25)
courier_order_sum	Выплата курьеру за заказы (по рейтингу)
courier_tips_sum	Сумма чаевых за месяц
courier_reward_sum	Итоговая сумма выплаты курьеру (courier_order_sum + courier_tips_sum * 0.95)


2. Список таблиц в слое DDS:
Существующие:  
    dds.dm_timestamp  
    dds.dm_restaurants  
    dds.dm_orders  
Создаем:  
    dds.dm_couriers  - источник	GET /couriers
    dds.dm_deliveries  - источник GET /deliveries

3. Список сущностей и полей, которые необходимо загрузить из API:  
    courier_id  
    courier_name  
    order_id  
    order_ts  
    delivery_id  
    delivery_ts  
    rate  
    tip_sum  
    sum

