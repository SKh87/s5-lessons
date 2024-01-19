drop table if exists public.outbox;

create table public.outbox
    (
        id serial constraint outbox_pk primary key ,
        object_id int not null ,
        record_ts timestamp not null ,
        type varchar not null ,
        payload text not null
);
