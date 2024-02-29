insert into stg.bonussystem_ranks(id, name, bonus_percent, min_payment_threshold)
-- values (:id, :name, :bonus_percent, :min_payment_threshold)
values (%(id)s, %(name)s, %(bonus_percent)s, %(min_payment_threshold)s)
on conflict (id) do update
    set name                  = excluded.name,
        bonus_percent         = excluded.bonus_percent,
        min_payment_threshold = excluded.min_payment_threshold;