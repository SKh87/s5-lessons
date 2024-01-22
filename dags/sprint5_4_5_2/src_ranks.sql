select *
from public.ranks
order by id asc
-- offset :offset limit :limit
 offset %(offset)s limit %(limit)s
--  offset ? limit ?
;