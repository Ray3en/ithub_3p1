-- Скрипт инкрементальной загрузки для SCD1
-- --------------------------------
-- Подготовка

create table st.xxxx_source (
	id integer,
	val varchar(50),
	update_dt timestamp(0)
);

insert into st.xxxx_source (id, val, update_dt) values (1, 'A', now());
insert into st.xxxx_source (id, val, update_dt) values (2, 'B', now());
insert into st.xxxx_source (id, val, update_dt) values (3, 'C', now());

insert into st.xxxx_source (id, val, update_dt) values (4, 'K', now());

update st.xxxx_source set val = 'H', update_dt =  now() where id = 3;

delete from st.xxxx_source where id = 1;

create table st.xxxx_stg (
	id integer,
	val varchar(50),
	update_dt timestamp(0),
	processed_dt timestamp(0)
);

create table st.xxxx_stg_del(
 	id integer
);

create table st.xxxx_target(
	id integer,
	val varchar(50),
	craeted_dt timestamp(0),
	update_dt timestamp(0),
	processed_dt timestamp(0)
);

create table st.xxxx_meta(
	schema_name varchar(30),
	table_name varchar(30),
	max_update_dt timestamp(0)
);

insert into st.xxxx_meta (schema_name, table_name, max_update_dt) 
values ('st', 'xxxx_source', to_date('1900-01-01', 'YYYY-MM-DD'));

-- Удобства для отладки
select * from st.xxxx_source;
select * from st.xxxx_stg;
select * from st.xxxx_stg_del;
select * from st.xxxx_target;
select * from st.xxxx_meta;


truncate  st.xxxx_source;
truncate  st.xxxx_target;

-- -----------------------------------------
-- Инкрементальная загрузка

-- 1. Очистка staging
truncate st.xxxx_stg;
truncate st.xxxx_stg_del;

-- 2. Захват данных из истончика в staging
insert into st.xxxx_stg (id, val, update_dt, processed_dt)
select 
	id, 
	val, 
	update_dt, 
	now() 
from st.xxxx_source
where update_dt > (
	select
		max_update_dt
	from st.xxxx_meta
	where table_name = 'xxxx_source' and schema_name = 'st'
);

insert into st.xxxx_stg_del (id)
select id from st.xxxx_source;


-- 3. Вставка данных в детальный слой 
insert into st.xxxx_target (id, val, craeted_dt, update_dt, processed_dt)
select
	stg.id,
	stg.val,
	stg.update_dt,
	null,
	now()
from st.xxxx_stg stg
left join st.xxxx_target tgt
on stg.id = tgt.id
where tgt.id is null;

-- 4. Обновление данных в детальном слое
update st.xxxx_target
set 
	val = tmp.val,
	update_dt = tmp.update_dt,
	processed_dt = now()
from (
	select
		stg.id,
		stg.val,
		stg.update_dt
	from st.xxxx_stg stg
	inner join st.xxxx_target tgt
	on stg.id = tgt.id
	where 
		1=0
		or stg.val <> tgt.val or (stg.val is null and tgt.val is not null) or (stg.val is not null and tgt.val is null) 
) tmp
where xxxx_target.id = tmp.id;

-- 5 Удлаение данных в детальном слое
delete from st.xxxx_target 
where id in (
	select 
		tgt.id
	from st.xxxx_target tgt
	left join st.xxxx_stg_del stg
	on tgt.id = stg.id
	where stg.id is null 
);


-- 6. Сохранить состояние загрузки в метаданных
update st.xxxx_meta 
set max_update_dt = coalesce((select max(update_dt) from st.xxxx_stg), now())
where table_name = 'xxxx_source' and schema_name = 'st';


-- 7. Фиксация транзакции
commit;