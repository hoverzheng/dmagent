#!/bin/bash
#

for s in `cat static_strings`
do
	#echo "$s"
	./dostatic.sh $s ./out  | awk -F: '{a[$1]+=$2} END{for(i in a) print i,a[i]}'
done

