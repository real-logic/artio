set terminal png size 1920,1080 enhanced font "Helvetica,20"
set output 'latencies.png'
set datafile separator ','
set ylabel "Latency in NS"
set xlabel "Time in NS"
set yrange [0:30000000]
plot 'latencies.csv' using 1:2 with lines, '' using 1:3 with lines

