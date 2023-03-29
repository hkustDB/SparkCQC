select g2.src, g2.dst, g3.dst, sum(c1.cnt)
from epinions g1, epinions g2, epinions g3,
     (select src, count(*) as cnt from epinions group by src) as c1,
     (select src, count(*) as cnt from epinions group by src) as c2
where g1.dst = g2.src and g2.dst = g3.src and g1.src = c1.src
  and g3.dst = c2.src and c1.cnt < c2.cnt
group by g2.src, g2.dst, g3.dst