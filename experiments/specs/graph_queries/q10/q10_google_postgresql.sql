select g3.src, g3.dst, count(*)
from google g1,
     google g2,
     google g3,
     (select src, count(*) as cnt from google group by src) as c1,
     (select src, count(*) as cnt from google group by src) as c2
where g1.dst = g2.src
  and g2.dst = g3.src
  and g1.src = c1.src
  and g3.dst = c2.src
  and c1.cnt < c2.cnt
group by g3.src, g3.dst