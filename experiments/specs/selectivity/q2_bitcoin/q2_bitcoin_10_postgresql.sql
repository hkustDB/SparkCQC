SELECT g1.src, g2.src, g3.src, g4.src, g5.src, g6.src
From bitcoin g1, bitcoin g2, bitcoin g3, bitcoin g4, bitcoin g5, bitcoin g6, bitcoin g7
WHERE g1.src = g3.dst AND g2.src = g1.dst AND g3.src=g2.dst
AND g4.src = g6.dst AND g5.src = g4.dst AND g6.src = g5.dst
AND g1.dst = g7.src AND g4.src = g7.dst
AND g1.weight*g2.weight*g3.weight + 10 < g4.weight*g5.weight*g6.weight