# SparkCQC

## Project Description

This project provide a simple demo of SparkCQC over the following 6 SQL queries:

### Query 1
```
select g1.src, g1.dst, g2.dst, g3.dst, c1.cnt, c2.cnt
from Graph g1, Graph g2, Graph g3,
(select src, count(*) as cnt from Graph group by src) as c1,
(select src, count(*) as cnt from Graph group by src) as c2
where g1.dst = g2.src and g2.dst = g3.src and g1.src = c1.src
and g3.dst = c2.src and c1.cnt < c2.cnt
```

### Query 2
```
SELECT g1.src, g2.src, g3.src, g4.src, g5.src, g6.src
From Graph g1, Graph g2, Graph g3, Graph g4, Graph g5, Graph g6, Graph g7
where g1.src = g3.dst and g2.src = g1.dst and g3.src=g2.dst
and g4.src = g6.dst and g5.src = g4.dst and g6.src = g5.dst
and g1.dst = g7.src and g4.src = g7.dst and
g1.weight*g2.weight*g3.weight < g4.weight*g5.weight*g6.weight
```

### Query 3
```
select g1.src, g1.dst, g2.dst, g3.dst, c1.cnt, c2.cnt, c3.cnt, c4.cnt
from Graph g1, Graph g2, Graph g3,
(select src, count(*) as cnt from Graph group by src) as c1,
(select src, count(*) as cnt from Graph group by src) as c2,
(select src, count(*) as cnt from Graph group by src) as c3
(select dst, count(*) as cnt from Graph group by dst) as c4,
where g1.dst = g2.src and g2.dst = g3.src and g1.src = c1.src
and g3.dst = c2.src and g3.dst = c4.dst and g2.src = c3.src
and c1.cnt < c2.cnt and c3.cnt < c4.cnt
```

### Query 4
```
select g3.src, g3.dst
from Graph g1, Graph g2, Graph g3,
(select src, count(*) as cnt from Graph group by src) as c1,
(select src, count(*) as cnt from Graph group by src) as c2
where g1.dst = g2.src and g2.dst = g3.src and g1.src = c1.src
and g3.dst = c2.src and c1.cnt < c2.cnt
```

### Query 5
```
SELECT * FROM Trade T1, Trade T2
WHERE T1.TT = "BUY" and T2.TT = "SALE"
and T1.CA_ID = T2.CA_ID
and T1.S_SYBM = T2.S_SYMB
and T1.T_DTS <= T2.T_DTS
and T1.T_DTS + interval '90' day >= T2.T_DTS
and T1.T_TRADE_PRICE*1.2 < T2.T_TRADE_PRICE
```

### Query 6
```
SELECT H1.CK, H2.CK, COUNT(DISTINCT H1.SK)
FROM Hold H1, Hold H2
WHERE H1.SK = H2. SK and H1.CK <> H2.CK
and H1.ST < H2.ET - interval '10' day
and H2.ST < H1.ET - interval '10' day
GROUP BY H1.CK, H2.CK
```

A formal package of SparkCQC will be released to open-source later. 

## System requirement

PostgreSQL:  You can directly import all tables and run the following SQL queries directly.

Spark 3.0.1:  You can refer to Spark Documentation for installing and running Spark program.

Maven/Java/Scala:  For compiling and debugging.  You can also import the project to IntelliJ IDEA for compiling.

 
