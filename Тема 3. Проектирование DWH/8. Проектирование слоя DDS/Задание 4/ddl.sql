drop table if exists dds.dm_products;
create table dds.dm_products
(
    id            serial
        constraint dm_products_pkey primary key,
    restaurant_id int            not null,
--         constraint dm_restaurants_dm_products_restaurant_id_fkey REFERENCES dds.dm_restaurants (id),
    product_id    varchar        not null,
    product_name  varchar        not null,
    product_price numeric(14, 2) not null default 0
        constraint dm_products_product_price_check check (product_price >= 0),
    active_from   timestamp      not null default now(),
    active_to     timestamp      not null default '3000-01-01'::timestamp
);