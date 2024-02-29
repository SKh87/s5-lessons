drop table if exists dds.fct_product_sales;
create table dds.fct_product_sales
(
    id            serial
        constraint fct_product_sales_pkey primary key,
    product_id    integer        not null,
    order_id      integer        not null,
    count         integer        not null default 0,
    price         numeric(14, 2) not null default 0,
    total_sum     numeric(14, 2) not null default 0,
    bonus_payment numeric(14, 2) not null default 0,
    bonus_grant   numeric(14, 2) not null default 0
);

alter table dds.fct_product_sales
    add constraint fct_product_sales_count_check check ( count >= 0 );
alter table dds.fct_product_sales
    add constraint fct_product_sales_price_check check ( price >= 0 );
alter table dds.fct_product_sales
    add constraint fct_product_sales_total_sum_check check ( total_sum >= 0 );
alter table dds.fct_product_sales
    add constraint fct_product_sales_bonus_payment_check check ( bonus_payment >= 0 );
alter table dds.fct_product_sales
    add constraint fct_product_sales_bonus_grant_check check ( bonus_grant >= 0 );
