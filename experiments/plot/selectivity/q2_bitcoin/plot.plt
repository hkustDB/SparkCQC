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
set xrange [0.0018:1]
set yrange [1:10000]
set xtics ("0.18\%%" 0.0018, "12.03\%%" 0.1203, "23.46\%%" 0.2346, "61.23\%%" 0.6123, "1" 1)
set key above
set grid lt 0 lc 0 lw 1
set border lw 2

set term pngcairo size 550,300
set output "output/figure/selectivity/q2_bitcoin.png"
# io, sparkcqc, sparksql, postgresql
plot "output/result/selectivity_q2_bitcoin/io.dat" using 1:($2/1000) title "I/O" ls 1 w lp, "output/result/selectivity_q2_bitcoin/sparkcqc.dat" using 1:($2/1000) title "SparkCQC" ls 2 w lp, "output/result/selectivity_q2_bitcoin/sparksql.dat" using 1:($2/1000) title "SparkSQL" ls 4 w lp, "output/result/selectivity_q2_bitcoin/postgresql.dat" using 1:($2/1000) title "PostgreSQL" ls 5 w lp
