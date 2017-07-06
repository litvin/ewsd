#!/bin/bash

dt=`find /u/phone/ewsd/store/ -mtime -7`

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

/root/newjasper/bin/ewsd2pg.py -t  >> /root/newjasper/log/ewsd2pg.log

rm -rf /root/newjasper/data/*

