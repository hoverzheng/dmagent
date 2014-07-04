#!/bin/bash
#
#monitor dmagent usage of memory.
#

#clear result file
>monitor_result

#echo file header
echo "pid VSZ RSS" >monitor_result

while [ 1 ]
do
    sleep 1
    #sleep 5
    #ps aux | grep "dmagent "| grep -v grep | awk '{print $2,$5,$6}' >>monitor_result
	echo "111">>dd
done
