-- run this before
-- create user bulker;
-- create database bulker owner bulker encoding 'UTF-8';

create type bulk_status_type as enum('new','sending','failed','finished');
create type bulk_subscription_filter_type as enum('all', 'subscribed', 'not_subscribed');
create type bulk_type as enum('subscribe', 'play', 'take_away_prize', 'other');

begin;

create table bulk (
    bulk_id serial not null primary key,
    bulk_type bulk_type not null default 'other',
    created timestamp without time zone not null default now(),
    active boolean not null default false,
    file text,
    subscription int not null default 0,
    subscription_filter bulk_subscription_filter_type not null default 'all',
    message text,
    start_stamp timestamp without time zone not null default 'infinity'::timestamp without time zone,
    status bulk_status_type not null default 'new',
    finish_stamp timestamp without time zone,
    start_time time without time zone not null default '11:00:00'::time,
    end_time time without time zone not null default '20:00:00'::time,
    load_limit int not null default 100,
    daily_limit int not null default 0
);

comment on table bulk is 'Разовые рассылки';
comment on column bulk.file is 'Пусть к файлу со списком абонентов';
comment on column bulk.subscription_filter is 'Фильтрация абонентов по признаку подписки (шлем всем, только подписчикам, только неподписанным)';
comment on column bulk.subscription is 'Если не 0, шлем только подписанным на данную подписку (subscription_filter дб subscribed)';
comment on column bulk.message is 'Текст отправляемого сообщения';
comment on column bulk.start_stamp is 'Дата/время начала отправки балка';
comment on column bulk.status is 'Статус балка (новый, рассылается, закончен, ошибка)';
comment on column bulk.finish_stamp is 'Дата/время завершения рассылки';
comment on column bulk.start_time is 'Время начала рассылки балка (по таймзоне абонента)';
comment on column bulk.end_time is 'Время окончания рассылки балка (по таймзоне абонента)';
comment on column bulk.load_limit is 'Кол-во загружаемых записей за один раз';
comment on column bulk.daily_limit is 'максимальное кол-во отправляемых сообщений в день, 0 - без ограничений)';

create table bulk_msisdn (
    bulk int not null references bulk(bulk_id),
    msisdn bigint not null,
    primary key (bulk, msisdn),
    sent boolean not null default false,
    sent_stamp timestamp without time zone,
    confirmed boolean not null default false,
    delivered boolean not null default false,
    played boolean not null default false,
    entered boolean not null default false,
    leaved boolean not null default false,
    taken_prize boolean not null default false,
    send_sms boolean not null default true
);
comment on table bulk_msisdn is 'Таблица абонентов для рассылки по конкретным балкам';
comment on column bulk_msisdn.sent is 'Сообщение поставлено в очередь';
comment on column bulk_msisdn.sent_stamp is 'Дата/время отправки сообщения абоненту';
comment on column bulk_msisdn.confirmed is 'Сообщение подтверждено SMSC';
comment on column bulk_msisdn.delivered is 'Сообщение доставлено абоненту';
comment on column bulk_msisdn.entered is 'Абонент подписался после рассылки';
comment on column bulk_msisdn.played is 'Абонент сыграл после рассылки';
comment on column bulk_msisdn.leaved is 'Абонент отписался после рассылки';
comment on column bulk_msisdn.taken_prize is 'Абонент забрал приз после рассылки';
comment on column bulk_msisdn.send_sms is 'Отправлять сообщение или нет (фильтрация)';

create table bulk_stat (
    day date not null,
    bulk int not null references bulk(bulk_id),
    primary key (day, bulk),
    sent int not null default 0,
    confirmed int not null default 0,
    delivered int not null default 0,
    entered int not null default 0,
    leaved int not null default 0,
    played int not null default 0,
    taken_prize int not null default 0,
    paid int not null default 0
);

create table visitor (
    msisdn bigint not null primary key,
    entered timestamp without time zone not null default now(),
    game_level int not null default 1
);
comment on table visitor is 'Абоненты';

create table subscription (
    subscription_id serial not null primary key,
    name text not null,
    game_level int
);
comment on table subscription is 'Подписки';
comment on column subscription.game_level is 'Уровень игры';

create table visitor_subscription (
    msisdn bigint not null,
    subscription int not null references subscription(subscription_id),
    created timestamp without time zone not null default now()
);
comment on table visitor_subscription is 'Подписки абонента';

create table timezone (
    timezone_id serial not null primary key,
    name text not null,
    msk_offset int not null default 0
);
comment on table timezone is 'Таймзоны';

create table region (
    code int not null primary key,
    name text not null,
    timezone int not null references timezone(timezone_id)
);
comment on table region is 'Таблица регионов с их идентификатором и таймзоной';

create table def_code (
    min bigint not null,
    max bigint not null,
    region int not null references region(code),
    operator text
);
create index def_code_min_max_region_idx on def_code using btree (min, max, region);
comment on table def_code is 'Таблица принадлежности пулов msisdn оператору и региону';







commit;

