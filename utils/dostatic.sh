#!/bin/bash
#

#./ketama_test >dd1 2>&1 
#awk '{print $3}' dd1 >dd2
#awk -F: '{a[$1]++} END{for(i in a) print i,a[i]}' out2

dostr="$1"
file="$2"

gstr_calloc="$dostr call"
gstr_free="$dostr free"

if [ ! $# -eq 2 ];
then
	echo "usage: $0 <str> <filename>"
	exit 0
fi


grep "$gstr_calloc" "$file"
grep "$gstr_free" "$file"


#grep "$gstr_calloc" "$file" | awk -F: '{a[$1]+=$2} END{for(i in a) print i,a[i]}'
#grep "$gstr_free" "$file" | awk -F: '{a[$1]+=$2} END{for(i in a) print i,a[i]}'
