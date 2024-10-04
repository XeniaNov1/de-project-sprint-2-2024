-- Добрый день! Спасибо что проверяете мою работу, мне тяжело далась тема с инкрементарным обновлением,
-- сделала как смогла и поняла, используя теорию и тренажер
-- я уверена у меня миллион ошибок
-- надеюсь на подробную обратную связь, чтобы я понимала где у меня ошибка и как ее исправить
-- спасибо!

-- Шаг 2. Изучение источника. У этих таблиц общий столбец - customer_id, по нему можно соединять таблицы
-- Шаг 3. Напишите скрипт переноса данных из источника в хранилище

DROP TABLE IF EXISTS merge_table;
CREATE TEMP TABLE merge_table as
SELECT  order_id,
        order_created_date,
        order_completion_date,
        order_status,
        craftsman_id,
        craftsman_name,
        craftsman_address,
        craftsman_birthday,
        craftsman_email,
        product_id,
        product_name,
        product_description,
        product_type,
        product_price,
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email 
  FROM source1.craft_market_wide
UNION
SELECT  t2.order_id,
        t2.order_created_date,
        t2.order_completion_date,
        t2.order_status,
        t1.craftsman_id,
        t1.craftsman_name,
        t1.craftsman_address,
        t1.craftsman_birthday,
        t1.craftsman_email,
        t1.product_id,
        t1.product_name,
        t1.product_description,
        t1.product_type,
        t1.product_price,
        t2.customer_id,
        t2.customer_name,
        t2.customer_address,
        t2.customer_birthday,
        t2.customer_email 
  FROM source2.craft_market_masters_products t1 
    JOIN source2.craft_market_orders_customers t2 ON t2.product_id = t1.product_id and t1.craftsman_id = t2.craftsman_id 
UNION
SELECT  t1.order_id,
        t1.order_created_date,
        t1.order_completion_date,
        t1.order_status,
        t2.craftsman_id,
        t2.craftsman_name,
        t2.craftsman_address,
        t2.craftsman_birthday,
        t2.craftsman_email,
        t1.product_id,
        t1.product_name,
        t1.product_description,
        t1.product_type,
        t1.product_price,
        t3.customer_id,
        t3.customer_name,
        t3.customer_address,
        t3.customer_birthday,
        t3.customer_email
  FROM source3.craft_market_orders t1
    JOIN source3.craft_market_craftsmans t2 ON t1.craftsman_id = t2.craftsman_id 
    JOIN source3.craft_market_customers t3 ON t1.customer_id = t3.customer_id
--добавление новых данных
UNION 
SELECT  ex1.order_id,
        ex1.order_created_date,
        ex1.order_completion_date,
        ex1.order_status,
        ex1.craftsman_id,
        ex1.craftsman_name,
        ex1.craftsman_address,
        ex1.craftsman_birthday,
       	ex1.craftsman_email,
        ex1.product_id,
        ex1.product_name,
        ex1.product_description,
        ex1.product_type,
        ex1.product_price,
        ex2.customer_id,
        ex2.customer_name,
        ex2.customer_address,
        ex2.customer_birthday,
        ex2.customer_email
	from external_source.craft_products_orders ex1
		join external_source.customers ex2 on ex1.customer_id=ex2.customer_id;


