alter table dds.dm_products
    drop constraint if exists dm_products_restaurant_id_fkey;
alter table dds.dm_products
    add constraint dm_products_restaurant_id_fkey foreign key (restaurant_id) references dds.dm_restaurants (id);