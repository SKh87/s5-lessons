drop table if exists dds.dm_restaurants;
create table dds.dm_restaurants(
    id serial constraint dm_restaurants_pkey primary key ,
    restaurant_id varchar not null ,
    restaurant_name varchar not null ,
    active_from timestamp not null default now(),
    active_to timestamp not null default '3000-01-01'::timestamp
);