SELECT DISTINCT g2.src, g2.dst
FROM google g1, google g2, google g3, google g4, google g5,
(SELECT src, COUNT(*) AS cnt FROM google GROUP BY src) AS c1,
(SELECT src, COUNT(*) AS cnt FROM google GROUP BY src) AS c2,
(SELECT dst, COUNT(*) AS cnt FROM google GROUP BY dst) AS c3,
(SELECT dst, COUNT(*) AS cnt FROM google GROUP BY dst) AS c4
WHERE g1.dst = g2.src AND g2.dst = g3.src AND g1.src = c1.src
AND g3.dst = c2.src AND c1.cnt < c2.cnt
AND g4.dst = g2.src AND g2.dst = g5.src AND g4.src = c3.dst
AND g5.dst = c4.dst AND c3.cnt < c4.cnt