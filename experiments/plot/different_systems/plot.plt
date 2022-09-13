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
# line style Willard's Approach
set style line 6 pt 3 lc 'yellow' ps 2 lw 2
# line style Any-K
set style line 7 pt 3 lc 'blue' ps 2 lw 2

set logscale y
set xlabel "Queries"
set ylabel "Processing Time (Sec)"
set xrange [-1:1]
set yrange [100:10000]
set xtics ("Q6" 0)
set ytics ("1e+02" 100, "1e+03" 1000, "1e+04" 10000)
set key above
set grid lt 0 lc 0 lw 1
set border lw 2

set style histogram cluster gap 3
set term pngcairo size 700,350
set output "output/figure/different_systems/result.png"
data = "output/result/different_systems/result.dat"
plot data u ($1/1000) ti "I/O" ls 1 w hist fs pattern 2 bor lc 'black', data u ($2/1000) ti "1D Alternative" ls 2 w hist fs pattern 1 bor lc 'black', data u ($3/1000) ti "SparkSQL" ls 4 w hist fs pattern 4 bor lc 'black', data u ($4/1000) ti "PostgreSQL" ls 5 w hist fs pattern 7 bor lc 'black', data u ($5/1000) ti "Willard's Approach" ls 6 w hist fs pattern 2 bor lc 'black', data u ($6/1000) ti "Any-K" ls 7 w hist fs pattern 1 bor lc 'black'