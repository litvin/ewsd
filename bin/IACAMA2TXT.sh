#!/bin/bash

dt=`find /u/phone/ewsd/store/ -mtime -7`
#dt=`ls /u/phone/ewsd/store/IAICAMA.*.2017042*` 
#dt=`find  /u/phone/ewsd/store/  -type f -newermt 2017-05-30 ! -newermt 2017-05-31`

#dt=`find  /u/phone/ewsd/store/ -iname IAICAMA.??.2017??31*` 
#dt=`find  /u/phone/ewsd/store/ -iname IAICAMA.??.20170601*`


cp $dt /root/newjasper/data/

for varname in `ls /root/newjasper/data/IAICAMA*.gz`
  do
        echo $varname
	newvarname=${varname//.gz/""}
	echo $newvarname        
	gzip -d $varname
        /opt/smartasr/bin/ewsd2txt $newvarname >> /root/newjasper/data/txt.tmp
	rm $newvarname
        mv /root/newjasper/data/txt.tmp $newvarname.txt
  done
