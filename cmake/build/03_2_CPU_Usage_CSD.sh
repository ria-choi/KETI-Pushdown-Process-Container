#!/bin/bash

pid=`ps -df | grep storage | awk '$8 == "./storage_engine_instance" {print $2}'`
time1=`date +%s%N`
utime=`cat /proc/$pid/stat | awk '{print $14}'`
stime=`cat /proc/$pid/stat | awk '{print $15}'`

before=$(expr $utime + $stime )

echo " > CPU Usage BEFORE Running Query : $before"
#### Query Processing ... ####
curl -X GET -H "Content-Type: application/json" -d '{"query":"TPC-H_04"}' http://10.0.5.119:34568/ --silent > /dev/null &
sleep 14
echo " > Start Running Query with CSD ... "

pid2=`ps -df | grep storage | awk '$8 == "./storage_engine_instance" {print $2}'`

utime2=`cat /proc/$pid2/stat | awk '{print $14}'`
stime2=`cat /proc/$pid2/stat | awk '{print $15}'`

after=$(expr $utime2 + $stime2 )
time2=`date +%s%N`
echo " > CPU Usage AFTER Running Query : $after"

echo "==> CSD CPU USAGE : $(expr $after - $before)"

echo "==> time : $(expr $time2 - $time1)"
