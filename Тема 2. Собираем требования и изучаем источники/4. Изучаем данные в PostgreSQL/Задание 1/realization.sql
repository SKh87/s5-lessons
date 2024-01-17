select
	table_name,
	column_name,
	data_type,
	character_maximum_length,
	column_default,
	is_nullable
from
	information_schema.columns c
where
	c.table_schema = 'public'
order by
	1,
	2;