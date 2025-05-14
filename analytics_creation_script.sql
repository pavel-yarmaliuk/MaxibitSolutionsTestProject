CREATE TABLE IF NOT EXISTS transactions(
	project VARCHAR(20) not null,
	id integer not null,
	user_id integer not null,
	created_at timestamp not null,
	amount numeric not null,
	currency varchar(10) not null,
	success boolean,
	primary key(project, id, user_id)
);

create index if not exists transactions_user_id_index on transactions(user_id);

create table if not exists exchange_rates(
	currency_from varchar(10) not null,
	currency_to varchar(10) not null,
	exchange_rate numeric not null,
	currency_date date not null,
	primary key(currency_from, currency_to, currency_date)
);



CREATE TABLE IF NOT EXISTS USER_SESSIONS  (
	PROJECT VARCHAR(20) not null,
	USER_ID INTEGER NOT NULL,
	ID INTEGER NOT NULL,
	ACTIVE BOOLEAN NOT NULL,
	PAGE_NAME VARCHAR(50) NOT NULL,
	LAST_ACTIVITY_AT TIMESTAMP NOT NULL,
	CREATED_AT TIMESTAMP NOT NULL,
	UPDATED_AT TIMESTAMP NOT NULL,
	PRIMARY KEY (PROJECT, USER_ID, ID)
);

CREATE TABLE IF NOT EXISTS USER_SESSIONS_RAW  (
	PROJECT VARCHAR(20) not null,
	USER_ID INTEGER NOT NULL,
	ID INTEGER NOT NULL,
	ACTIVE BOOLEAN NOT NULL,
	PAGE_NAME VARCHAR(50) NOT NULL,
	LAST_ACTIVITY_AT TIMESTAMP NOT NULL,
	CREATED_AT TIMESTAMP NOT NULL,
	UPDATED_AT TIMESTAMP NOT NULL,
	PRIMARY KEY (PROJECT, USER_ID, ID)
);


CREATE TABLE IF NOT EXISTS PAGES  (
	ID INTEGER NOT null PRIMARY key,
	NAME VARCHAR(50) NOT NULL,
	CREATED_AT TIMESTAMP NOT null
);


CREATE TABLE IF NOT EXISTS EVENTS  (
	USER_ID INTEGER NOT NULL,
	ID INTEGER NOT NULL,
	PROJECT VARCHAR(20) not null,
	EVENT_NAME VARCHAR(50) NOT NULL,
	PAGE_ID INTEGER references pages (id) NOT NULL,
	CREATED_AT TIMESTAMP NOT NULL,
	PRIMARY KEY (USER_ID, ID, PROJECT)
);

CREATE TABLE IF NOT EXISTS EVENTS_RAW  (
	USER_ID INTEGER NOT NULL,
	ID INTEGER NOT NULL,
	PROJECT VARCHAR(20) not null,
	EVENT_NAME VARCHAR(50) NOT NULL,
	PAGE_ID INTEGER references pages (id) NOT NULL,
	CREATED_AT TIMESTAMP NOT NULL,
	PRIMARY KEY (USER_ID, ID, PROJECT)
);

create materialized view user_sessions_with_events_count as
select us.project, us.user_id, us.id, count(1) as amount_of_events_in_session
from user_sessions us
join events e on us.user_id = e.user_id and us.project = e.project
where e.created_at >= us.created_at and e.created_at <= us.last_activity_at
group by us.project, us.user_id, us.id;



create materialized view transactions_in_usd as
select t.amount * er.exchange_rate as amount_in_usd, t.success as success, 'USD' as currency, t.created_at as created_at, t.user_id as user_id, t.id as id, t.project as project
from transactions t
join exchange_rates er on t.currency = er.currency_from
where er.currency_to = 'USD' and er.currency_date = t.created_at::date;


create materialized view first_successful_transaction as
WITH partitioned_sessions_with_transactions AS (
	select us.user_id, us.project, us.id, t.created_at, row_number() over(partition by us.id, us.project order by t.created_at) as rn
	from user_sessions us
	join transactions t on us.user_id = t.user_id and us.project = t.project
	where t.success = true
)
select user_id, project, id, created_at from partitioned_sessions_with_transactions
where rn = 1;

create materialized view first_successful_transaction_in_usd as
WITH partitioned_sessions_with_transactions AS (
	select us.user_id, us.project, us.id, t.created_at, row_number() over(partition by us.id, us.project order by t.created_at) as rn
	from user_sessions us
	join transactions t on us.user_id = t.user_id and us.project = t.project
	where t.success = true and t.currency = 'USD'
)
select user_id, project, id, created_at from partitioned_sessions_with_transactions
where rn = 1;

create materialized view sum_of_transactions_in_session as
select us.user_id, us.id, us.project, sum(t.amount_in_usd) as sum_in_usd
from user_sessions us
join transactions_in_usd t on us.user_id = t.user_id and us.project = t.project
where t.created_at >= us.created_at  and t.created_at <= us.updated_at and t.success = true
group by (us.user_id, us.id, us.project);


create materialized view session_analytics as
select so.project, so.id, so.user_id, so.sum_in_usd, fs.created_at as first_successful_transaction, fst.created_at as first_successful_transaction_in_usd
from sum_of_transactions_in_session so
left join first_successful_transaction fs on so.id = fs.id and so.project = fs.project and so.user_id = fs.user_id
left join first_successful_transaction_in_usd fst on so.id = fst.id and so.project = fst.project and so.user_id = fst.user_id;
