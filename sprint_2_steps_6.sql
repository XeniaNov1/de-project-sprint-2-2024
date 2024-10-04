--Шаг 6. инкрементарное обновление
	
-- здесь я разбила запрос на шаги как в обучении т.к. писать весь массив запроса сразу мне тяжело

--1. Создать дополнительную таблицу
--В дополнительной таблице — две основные колонки: id записи, дата и время загрузки новых данных.
-- Дата и время загрузки будут определяться как максимальное время из всего, что было загружено в хранилище.
-- На основе даты вы будете принимать решение о том, какие данные были изменены или добавлены
	
DROP TABLE IF EXISTS dwh.load_dates_external_source;
CREATE TABLE IF NOT EXISTS dwh.load_dates_external_source (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    load_dttm DATE NOT NULL,
    CONSTRAINT load_dates_external_source_pk PRIMARY KEY (id)
);

-- 2. Выбрать из хранилища только изменённые или новые данные
-- Чтобы определить, какие данные были изменены или добавлены, нужно добавить в запрос следующее условие: 
-- дата загрузки данных в DWH должна быть старше даты из дополнительной таблицы. 

drop table if exists table_2;
create table if not exists table_2 as 
select
	dcs.customer_id,
	dcs.customer_name,
	dcs.customer_address,
	dcs.customer_birthday,
	dcs.customer_email,
	v.sum_paid,
	v.sum_got,
	v.q_orders,
	v.av_sum_per_order,
	v.median_days_complete_order,
	v.count_order_created,
	v.count_order_in_progress,
	v.count_order_delivery,
	v.count_order_done,
	v.count_order_not_done,
	ext.customer_id as exist_customer_id,
	dcs.load_dttm as customers_load_dttm
from vitrina v
	inner join dwh.d_customer dcs on dcs.customer_id=v.customer_id
	left join external_source.customers ext on v.customer_id = ext.customer_id
where dcs.load_dttm > (select COALESCE(MAX(load_dttm),'1900-01-01') from dwh.load_dates_external_source);

-- 3. Определить, какие данные из дельты нужно обновить
--Части данных в витрине раньше не было, поэтому их нужно вставить с помощью INSERT,
-- а другая часть уже была — их значения нужно обновить с помощью UPDATE.

-- создаём таблицу table_3
DROP TABLE IF EXISTS table_3;
CREATE TABLE IF NOT EXISTS table_3 AS (
	SELECT 	customer_id		
			FROM table_2 t2
				WHERE t2.exist_customer_id = customer_id                         
);

-- 4. Выполнить расчёт витрины только для данных, которые нужно вставить
--В table_3 находятся customer_id, которые нужно обновить. 
--А те данные, что нужно добавить, — в блоке table_2 с exist_craftsman_id is NULL

DROP TABLE IF EXISTS table_4;
CREATE TABLE IF NOT EXISTS table_4 as

with main_table as
	(select fo.order_id, -- идентификатор записи;
		t3.customer_id,  -- идентификатор заказчика;
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
		TO_CHAR(fo.order_created_date, 'yyyy-mm') as report_period, -- отчётный период, год и месяц
		dcs.load_dttm as customers_load_dttm
	FROM dwh.f_order fo 
		--INNER JOIN dwh.d_craftsman dc ON dc.craftsman_id=fo.craftsman_id
		INNER JOIN dwh.d_customer dcs ON dcs.customer_id=fo.customer_id
		INNER JOIN dwh.d_product dp ON dp.product_id=fo.product_id
		inner join table_3 t3 on fo.customer_id=t3.customer_id
	group by t3.customer_id,fo.order_id, dcs.customer_name, dcs.customer_address,dcs.customer_birthday,dcs.customer_email,dcs.load_dttm),

-- ищем топ категорию
top_category as (
	select 
		t3.customer_id as customer_id,	
		RANK() OVER(PARTITION BY t3.customer_id ORDER BY COUNT(dp.product_type) DESC) AS top_product,
		dp.product_type as product_type,
		COUNT(dp.product_type) as q_products
	from dwh.f_order fo
		inner join dwh.d_customer dcs on dcs.customer_id=fo.customer_id
		inner join dwh.d_product dp on dp.product_id=fo.product_id
		inner join table_3 t3 on fo.customer_id=t3.customer_id
	group by t3.customer_id, dp.product_type),

-- ищем топ мастера
top_craftsman as (
	select
		t3.customer_id as customer_id,
		RANK() OVER(PARTITION BY t3.customer_id ORDER BY COUNT(dc.craftsman_id) DESC) AS top_craftsman,
		dc.craftsman_id as craftsman_id,
		count(dc.craftsman_id) as q_craftsman
	from dwh.f_order fo
		inner join dwh.d_customer dcs on dcs.customer_id=fo.customer_id
		inner join dwh.d_craftsman dc ON dc.craftsman_id=fo.craftsman_id
		inner join table_3 t3 on fo.customer_id=t3.customer_id
	group by t3.customer_id, dc.craftsman_id)

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
	inner join top_craftsman tcr on mt.customer_id = tcr.customer_id;
	
-- 5. Выполнить расчёт витрины для данных, которые нужно обновить

DROP TABLE IF EXISTS table_5;
CREATE TABLE IF NOT EXISTS table_5 as

with main_table as
	(select fo.order_id, -- идентификатор записи;
		t2.customer_id,  -- идентификатор заказчика;
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
		inner join table_2 t2 on fo.customer_id=t2.customer_id
	where t2.exist_customer_id = null
	group by t2.customer_id,fo.order_id, dcs.customer_name, dcs.customer_address,dcs.customer_birthday,dcs.customer_email,dcs.load_dttm),

-- ищем топ категорию
top_category as (
	select 
		t2.customer_id as customer_id,	
		RANK() OVER(PARTITION BY t2.customer_id ORDER BY COUNT(dp.product_type) DESC) AS top_product,
		dp.product_type as product_type,
		COUNT(dp.product_type) as q_products
	from dwh.f_order fo
		inner join dwh.d_customer dcs on dcs.customer_id=fo.customer_id
		inner join dwh.d_product dp on dp.product_id=fo.product_id
		inner join table_2 t2 on fo.customer_id=t2.customer_id
	where t2.exist_customer_id = null
	group by t2.customer_id, dp.product_type),

-- ищем топ мастера
top_craftsman as (
	select
		t2.customer_id as customer_id,
		RANK() OVER(PARTITION BY t2.customer_id ORDER BY COUNT(dc.craftsman_id) DESC) AS top_craftsman,
		dc.craftsman_id as craftsman_id,
		count(dc.craftsman_id) as q_craftsman
	from dwh.f_order fo
		inner join dwh.d_customer dcs on dcs.customer_id=fo.customer_id
		inner join dwh.d_craftsman dc ON dc.craftsman_id=fo.craftsman_id
		inner join table_2 t2 on fo.customer_id=t2.customer_id
	where t2.exist_customer_id = null
	group by t2.customer_id, dc.craftsman_id)

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
		mt.customers_load_dttm,
		tcat.product_type as top_product_category,
		tcr.craftsman_id as top_craftsman_id
	from main_table mt
	inner join top_category tcat on mt.customer_id = tcat.customer_id
	inner join top_craftsman tcr on mt.customer_id = tcr.customer_id;
	
-- 6. Выполнить вставку новых данных в витрину
	
	insert into vitrina (
		order_id,
		customer_id,
		customer_name,
		customer_address,
		customer_birthday,
		customer_email,
		sum_paid,
		sum_got,
		q_orders,
		av_sum_per_order,
		median_days_complete_order,
		count_order_created,
		count_order_in_progress,
		count_order_delivery,
		count_order_done,
		count_order_not_done,
		report_period,
		top_product_category,
		top_craftsman_id,
		customers_load_dttm)
		select 
			order_id,
			customer_id,
			customer_name,
			customer_address,
			customer_birthday,
			customer_email,
			sum_paid,
			sum_got,
			q_orders,
			av_sum_per_order,
			median_days_complete_order,
			count_order_created,
			count_order_in_progress,
			count_order_delivery,
			count_order_done,
			count_order_not_done,
			report_period,
			top_product_category,
			top_craftsman_id,
			customers_load_dttm
		from table_4;
		
-- шаг 7. Выполнить обновление изменённых данных в витрине
		update vitrina set
			order_id=updates.order_id,
			customer_id=updates.customer_id,
			customer_name=updates.customer_name,
			customer_address=updates.customer_address,
			customer_birthday=updates.customer_birthday,
			customer_email=updates.customer_email,
			sum_paid=updates.sum_paid,
			sum_got=updates.sum_got,
			q_orders=updates.q_orders,
			av_sum_per_order=updates.av_sum_per_order,
			median_days_complete_order=updates.median_days_complete_order,
			count_order_created=updates.count_order_created,
			count_order_in_progress=updates.count_order_in_progress,
			count_order_delivery=updates.count_order_delivery,
			count_order_done=updates.count_order_done,
			count_order_not_done=updates.count_order_not_done,
			report_period=updates.report_period,
			top_product_category=updates.top_product_category,
			top_craftsman_id=updates.top_craftsman_id,
			customers_load_dttm = updates.customers_load_dttm
			from (select 
					order_id,
					customer_id,
					customer_name,
					customer_address,
					customer_birthday,
					customer_email,
					sum_paid,
					sum_got,
					q_orders,
					av_sum_per_order,
					median_days_complete_order,
					count_order_created,
					count_order_in_progress,
					count_order_delivery,
					count_order_done,
					count_order_not_done,
					report_period,
					top_product_category,
					top_craftsman_id,
					customers_load_dttm
				  from table_5) as updates
				where vitrina.customer_id=updates.customer_id;

-- 8. Выполнить вставку максимальной даты загрузки из дельты в дополнительную таблицу
-- делаем запись в таблицу загрузок о том, когда была совершена загрузка, 
--чтобы в следующий раз взять данные, которые будут добавлены или изменены после этой даты				

insert into vitrina (
		customers_load_dttm
	)
	select 
        greatest(
            coalesce(max(customers_load_dttm), now()))
    from table_2