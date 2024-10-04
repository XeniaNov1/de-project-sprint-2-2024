
-- Шаг 5. Напишите DDL новой витрины

-- создаем таблицу 
drop table if exists vitrina;
create table if not exists vitrina as 
--делаем основную часть витрины
with main_table as
	(select fo.order_id, -- идентификатор записи;
		dcs.customer_id,  -- идентификатор заказчика;
		dcs.customer_name, -- Ф. И. О. заказчика;
		dcs.customer_address, -- адрес заказчика;
		dcs.customer_birthday, -- дата рождения заказчика;
		dcs.customer_email, -- электронная почта заказчика;
		SUM (dp.product_price) as sum_paid, -- сумма, которую потратил заказчик;
		SUM (dp.product_price)*0.1 as sum_got, -- сумма, которую заработала платформа от покупок заказчика за месяц (10% от суммы, которую потратил заказчик);
		COUNT(fo.order_id) as q_orders, -- количество заказов у заказчика за месяц;
		AVG(dp.product_price) as av_sum_per_order, -- средняя стоимость одного заказа у заказчика за месяц;
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY fo.order_completion_date - fo.order_created_date) AS median_days_complete_order, -- медианное время в днях от момента создания заказа до его завершения за месяц;
		COUNT(fo.order_id) AS count_order_created,-- количество созданных заказов за месяц;
		SUM(CASE WHEN fo.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, -- количество заказов в процессе изготовки за месяц;
		SUM(CASE WHEN fo.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, -- количество заказов в доставке за месяц;
		SUM(CASE WHEN fo.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, -- количество завершённых заказов за месяц;
		SUM(CASE WHEN fo.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done, -- количество незавершённых заказов за месяц;
		TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period, -- отчётный период, год и месяц
		dcs.load_dttm as customers_load_dttm
	FROM dwh.f_order fo 
		--INNER JOIN dwh.d_craftsman dc ON dc.craftsman_id=fo.craftsman_id
		INNER JOIN dwh.d_customer dcs ON dcs.customer_id=fo.customer_id
		INNER JOIN dwh.d_product dp ON dp.product_id=fo.product_id
	group by dcs.customer_id,fo.order_id),

-- ищем топ категорию
top_category as (
	select 
		dcs.customer_id as customer_id,	
		RANK() OVER(PARTITION BY dcs.customer_id ORDER BY COUNT(dp.product_type) DESC) AS top_product,
		dp.product_type as product_type,
		COUNT(dp.product_type) as q_products
	from dwh.f_order fo
		inner join dwh.d_customer dcs on dcs.customer_id=fo.customer_id
		inner join dwh.d_product dp on dp.product_id=fo.product_id
		group by dcs.customer_id, dp.product_type),

-- ищем топ мастера
top_craftsman as (
	select
		dcs.customer_id as customer_id,
		RANK() OVER(PARTITION BY dcs.customer_id ORDER BY COUNT(dc.craftsman_id) DESC) AS top_craftsman,
		dc.craftsman_id as craftsman_id,
		count(dc.craftsman_id) as q_craftsman
	from dwh.f_order fo
		inner join dwh.d_customer dcs on dcs.customer_id=fo.customer_id
		inner join dwh.d_craftsman dc ON dc.craftsman_id=fo.craftsman_id
	group by dcs.customer_id, dc.craftsman_id)

--делаем витрину
select
		mt.order_id,
		mt.customer_id,
		mt.customer_name,
		mt.customer_address,
		mt.customer_birthday,
		mt.customer_email,
		mt.sum_paid,
		mt.sum_got,
		mt.q_orders,
		mt.av_sum_per_order,
		mt.median_days_complete_order,
		mt.count_order_created,
		mt.count_order_in_progress,
		mt.count_order_delivery,
		mt.count_order_done,
		mt.count_order_not_done,
		mt.report_period,
		tcat.product_type as top_product_category,
		tcr.craftsman_id as top_craftsman_id,
		mt.customers_load_dttm
	from main_table mt
	inner join top_category tcat on mt.customer_id = tcat.customer_id
	inner join top_craftsman tcr on mt.customer_id = tcr.customer_id