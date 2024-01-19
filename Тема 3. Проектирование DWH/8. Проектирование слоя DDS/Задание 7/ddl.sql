drop table if exists dds.dm_orders;
create table dds.dm_orders
(
    id            serial
        constraint dm_orders_pkey primary key,
    order_key     varchar not null,
    order_status  varchar not null,
    restaurant_id integer not null,
    timestamp_id  integer not null,
    user_id       integer not null
);