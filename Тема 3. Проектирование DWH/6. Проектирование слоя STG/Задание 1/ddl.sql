drop table if exists stg.outbox;
drop table if exists stg.ranks;
drop table if exists stg.users;
drop schema if exists stg;

create schema stg;

create table stg.bonussystem_events
(
    id          int4      not null,
    event_ts    timestamp not null,
    event_type  varchar   not null,
    event_value text      not null,
    constraint outbox_pkey primary key (id)
);
create index idx_bonussystem_events__event_ts on stg.bonussystem_events using btree (event_ts);

create table stg.bonussystem_ranks
(
    id                    int4           not null,
    "name"                varchar(2048)  not null,
    bonus_percent         numeric(19, 5) not null default 0,
    min_payment_threshold numeric(19, 5) not null default 0,
    constraint bonussystem_ranks_bonus_percent_check check ((bonus_percent >= (0)::numeric)),
    constraint bonussystem_ranks_bonus_percent_check1 check ((bonus_percent >= (0)::numeric)),
    constraint bonussystem_ranks_pkey primary key (id)
);

create table stg.bonussystem_users
(
    id            int4 not null,
    order_user_id text not null,
    constraint bonussystem_users_pkey primary key (id)
);