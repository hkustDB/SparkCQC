SELECT g1.src, g1.dst, g2.dst, g3.dst, c1.cnt, c2.cnt
FROM epinions g1, epinions g2, epinions g3,
(SELECT src, COUNT(*) AS cnt FROM epinions GROUP BY src) AS c1,
(SELECT src, COUNT(*) AS cnt FROM epinions GROUP BY src) AS c2
WHERE g1.dst = g2.src AND g2.dst = g3.src AND g1.src = c1.src AND g3.dst = c2.src
AND c1.cnt + 100 < c2.cnt