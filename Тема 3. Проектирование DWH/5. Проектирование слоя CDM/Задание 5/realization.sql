alter table cdm.dm_settlement_report
    drop constraint if exists dm_settlement_report_restaurant_id_settlement_date_ex;

alter table cdm.dm_settlement_report
    add constraint dm_settlement_report_restaurant_id_settlement_date_ex unique (restaurant_id, settlement_date);