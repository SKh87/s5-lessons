select distinct
	jsonb_array_elements(event_value ::jsonb ->'product_payments')->>'product_name'
from
	outbox o
where
	1 = 1
	and event_value ::jsonb ->>'product_payments' is not null
order by 1;

