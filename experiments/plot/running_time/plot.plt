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
set xlabel "Queries"
set ylabel "Processing Time (Sec)"
set xrange [-1:15]
set yrange [0.1:100000]
set xtics ("Q1-Epinions" 0, "Q1-Google" 1, "Q1-Wiki" 2, "Q2-Bitcoin" 3, "Q3-Epinions" 4, "Q3-Google" 5, "Q3-Wiki" 6, "Q4-Epinions" 7, "Q4-Google" 8, "Q4-Wiki" 9, "Q5-DBLP" 10, "Q5-Google" 11, "Q6" 12, "Q7" 13, "Q8" 14)
set ytics ("1e-01" 0.1, "1e+00" 1, "1e+01" 10, "1e+02" 100, "1e+03" 1000, "1e+04" 10000, "1e+05" 100000)
set key above
set grid lt 0 lc 0 lw 1
set border lw 2

set style histogram cluster gap 3
set term pngcairo size 2100,350
set output "output/figure/running_time/result.png"
data = "output/result/running_time/result.dat"
plot data u ($1/1000) ti "I/O" ls 1 w hist fs pattern 2 bor lc 'black', data u ($2/1000) ti "SparkCQC" ls 2 w hist fs pattern 1 bor lc 'black', data u ($3/1000) ti "SparkSQL" ls 4 w hist fs pattern 4 bor lc 'black', data u ($4/1000) ti "PostgreSQL" ls 5 w hist fs pattern 7 bor lc 'black'