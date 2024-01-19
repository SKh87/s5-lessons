alter table cdm.dm_settlement_report
    drop constraint if exists dm_settlement_report_pk;

alter table cdm.dm_settlement_report
    add constraint dm_settlement_report_pk primary key (id);