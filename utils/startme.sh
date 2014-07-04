#!/bin/bash
#

ma_port=12000

>out
killall -9 dmagent
killall -9 dmagent
#./dmagent -v -k -u root -n 51200 -p $ma_port -c dmagent.conf
#./dmagent -v -k -u root -n 51200 -p $ma_port -c dmagent.conf >/dev/null 2>&1
#./dmagent -v -k -u root -n 51200 -p $ma_port -c dmagent.conf >out 2>&1
#./dmagent -v -u root -n 51200 -p $ma_port -c dmagent.conf >out 2>&1
#./dmagent -u root -n 51200 -p $ma_port -c dmagent.conf >/dev/null 2>&1
#./dmagent -v -u root -n 51200 -p $ma_port -c dmagent.conf && echo "start ok!" && ps -ef | grep dmagent
#./dmagent -u root -n 51200 -p $ma_port -c dmagent.conf >/dev/null 2>&1

#./dmagent -v -u root -n 51200 -p $ma_port -c dmagent.conf >out 2>&1 &
#./dmagent -v -u root -n 51200 -p $ma_port -c dmagent.conf 
./dmagent -u root -n 51200 -p $ma_port -c dmagent.conf 
ps aux | grep dmagent | grep -v grep && echo "dmagent start successfully!"
