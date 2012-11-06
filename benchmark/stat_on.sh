cd $1 
vmstat -n 60 > vmstat$2.txt &
echo $! > /tmp/vmstat.pid

iostat 60 > iostat$2.txt &
echo $! > /tmp/iostat.pid
