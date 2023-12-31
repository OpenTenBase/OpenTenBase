#!/bin/bash

# This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
# All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.

if [ ! -f isolation_test.conf ] ; then
    exit 1;
fi

key_array=()
value_array=()
schedual_file=txn_test_schedual

i=0
for line_buf in `cat ./isolation_test.conf | grep -v ^"#" | sed 's/[[:space:]]*//g'`
do
    key_array[i]=`echo ${line_buf} | awk -F '=' '{print $1}'`
    value_array[i]=`echo ${line_buf} | awk -F '=' '{print $2}'`
    i=$((i+1))
done

#debug
#i=0
#num=${#key_array[@]}
#while [ $i -lt $num ]; do
#
#    echo   ${key_array[i]} - ${value_array[i]};
#    i=$((i+1))
#done

if [ -f $schedual_file ]; then
    rm $schedual_file
fi

for file_src in `ls ./isolation_test_src/*.src` 
do 
    file_dst=`echo ${file_src##*/}|sed 's/\.src/\.spec/g'`
    file_for_schedual=`echo ${file_src##*/}|sed 's/\.src//g'`
    #echo $file_dst
    
    if [ $file_dst = '' ] ;then 
        continue
    fi
    
    if [ -f ./specs/$file_dst ]; then
        rm ./specs/$file_dst
    fi
    
    echo treat file : $file_src
    cat $file_src| while read line_buf
    do
        i=0
        num=${#key_array[@]}
        while [ $i -lt $num ]; do
            line_buf=`echo $line_buf|sed "s/${key_array[i]}/${value_array[i]}/g"`
            i=$((i+1))
        done
    
        echo "$line_buf" >> "./specs/$file_dst"
    done
    echo "test: $file_for_schedual" >> $schedual_file
done

echo "done"
exit 0
