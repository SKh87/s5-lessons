drop table if exists stg.ordersystem_orders;
create table stg.ordersystem_orders
(
    id           serial primary key,
    object_id    varchar not null,
    object_value text not null,
    update_ts    timestamp not null
);

drop table if exists stg.ordersystem_restaurants;
create table stg.ordersystem_restaurants
(
    id           serial primary key,
    object_id    varchar not null,
    object_value text not null,
    update_ts    timestamp not null
);

drop table if exists stg.ordersystem_users;
create table stg.ordersystem_users
(
    id           serial primary key,
    object_id    varchar not null,
    object_value text not null,
    update_ts    timestamp not null
);


-- id — уникальный идентификатор записи в таблице типа serial. Поле также должно быть первичным ключом.
-- object_id — понадобится поле типа varchar для хранения уникального идентификатора документа из MongoDB.
-- object_value — нужно поле типа text, в котором будет храниться весь документ из MongoDB.
-- update_ts — поле типа timestamp, в котором нужно сохранить update_ts исходного объекта из MongoDB. Это поле позволит сразу отличать новые заказы от старых и будет использоваться вами при обработке данных.