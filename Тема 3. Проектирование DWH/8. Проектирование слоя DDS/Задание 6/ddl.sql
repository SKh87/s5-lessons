drop table if exists dds.dm_timestamps;
create table dds.dm_timestamps
(
    id    serial
        constraint dm_timestamps_pkey primary key,
    ts    timestamp not null,
    year  smallint  not null
        constraint dm_timestamps_year_check check ( year >= 2022 and year < 2500 ),
    month smallint  not null
        constraint dm_timestamps_month_check check ( month >= 1 and month <= 12 ),
    day   smallint  not null
        constraint dm_timestamps_day_check check ( day >= 1 and day <= 31 ),
    time  time      not null,
    date  date      not null
);