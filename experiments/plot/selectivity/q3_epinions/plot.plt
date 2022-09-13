# line style I/O
set style line 1 pt 4 lc 'black' ps 2 lw 2
# line style CQC plan1
set style line 2 pt 1 lc 'forest-green' ps 2 lw 2
# line style CQC plan2
set style line 3 pt 6 lc 'purple' ps 2 lw 2
# line style SparkSQL
set style line 4 pt 2 lc 'skyblue' ps 2 lw 2
# line style PostgreSQL
set style line 5 pt 3 lc 'orange' ps 2 lw 2

set logscale y
set xlabel "Percentage of Output Size Compares with k=0"
set ylabel "Processing Time (Sec)"
set xrange [0.0039:1]
set yrange [1:10000]
set xtics ("0.39\%%" 0.0039, "42.50\%%" 0.425, "70.36\%%" 0.7036, "94.74\%%" 0.9474, "1" 1)
set key above
set grid lt 0 lc 0 lw 1
set border lw 2

set term pngcairo size 700,350
set output "output/figure/selectivity/q3_epinions.png"
# io, sparkcqc1, sparkcqc2, sparksql, postgresql
plot "output/result/selectivity_q3_epinions/io.dat" using 1:($2/1000) title "I/O" ls 1 w lp, "output/result/selectivity_q3_epinions/sparkcqc1.dat" using 1:($2/1000) title "Plan1" ls 2 w lp, "output/result/selectivity_q3_epinions/sparkcqc2.dat" using 1:($2/1000) title "Plan2" ls 3 w lp, "output/result/selectivity_q3_epinions/sparksql.dat" using 1:($2/1000) title "SparkSQL" ls 4 w lp, "output/result/selectivity_q3_epinions/postgresql.dat" using 1:($2/1000) title "PostgreSQL" ls 5 w lp
