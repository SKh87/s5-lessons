with test_01 as (select a.id as aid, dsre.id as eid
                 from public_test.dm_settlement_report_actual a
                          full join public_test.dm_settlement_report_expected dsre on a.id = dsre.id and
                                                                                      a.restaurant_id =
                                                                                      dsre.restaurant_id and
                                                                                      a.restaurant_name =
                                                                                      dsre.restaurant_name and
                                                                                      a.settlement_year =
                                                                                      dsre.settlement_year and
                                                                                      a.settlement_month =
                                                                                      dsre.settlement_month and
                                                                                      a.orders_count =
                                                                                      dsre.orders_count and
                                                                                      a.orders_total_sum =
                                                                                      dsre.orders_total_sum and
                                                                                      a.orders_bonus_payment_sum =
                                                                                      dsre.orders_bonus_payment_sum and
                                                                                      a.orders_bonus_granted_sum =
                                                                                      dsre.orders_bonus_granted_sum and
                                                                                      a.order_processing_fee =
                                                                                      dsre.order_processing_fee and
                                                                                      a.restaurant_reward_sum =
                                                                                      dsre.restaurant_reward_sum)
select now()           test_date_time,
       'test_01'    as test_name,
       count(*) = 0 as test_result
from test_01
where aid is null
   or eid is null
;