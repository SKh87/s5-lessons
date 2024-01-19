insert into cdm.dm_settlement_report(restaurant_id,
                                     restaurant_name,
                                     settlement_date,
                                     orders_count,
                                     orders_total_sum,
                                     orders_bonus_payment_sum,
                                     orders_bonus_granted_sum,
                                     order_processing_fee,
                                     restaurant_reward_sum)
select dr.restaurant_id,
       dr.restaurant_name,
       dt.date                                                               as settlement_date,
       count(distinct o.id)                                                     as orders_count,
       sum(fct.total_sum)                                                    as orders_total_sum,
       sum(fct.bonus_payment)                                                as orders_bonus_payment_sum,
       sum(fct.bonus_grant)                                                  as orders_bonus_granted_sum,
       sum(fct.total_sum) * 0.25                                            as order_processing_fee,
       sum(fct.total_sum) * 0.75 - sum(fct.bonus_payment) as restaurant_reward_sum
from dds.fct_product_sales fct
         inner join dds.dm_orders o
                    on o.id = fct.order_id
         inner join dds.dm_timestamps dt
                    on dt.id = o.timestamp_id
         inner join dds.dm_users du
                    on du.id = o.user_id
         inner join dds.dm_products dp
                    on dp.id = fct.product_id
         inner join dds.dm_restaurants dr
                    on dr.id = dp.restaurant_id
                        and dr.id = o.restaurant_id
where o.order_status = 'CLOSED'
group by dr.restaurant_id, dr.restaurant_name, dt.date
on conflict on constraint dm_settlement_report_restaurant_id_settlement_date_ex
    do update set restaurant_name          = excluded.restaurant_name,
                  orders_count             = excluded.orders_count,
                  orders_total_sum         = excluded.orders_total_sum,
                  orders_bonus_payment_sum = excluded.orders_bonus_payment_sum,
                  orders_bonus_granted_sum = excluded.orders_bonus_granted_sum,
                  order_processing_fee     = excluded.order_processing_fee,
                  restaurant_reward_sum    = excluded.restaurant_reward_sum
;







