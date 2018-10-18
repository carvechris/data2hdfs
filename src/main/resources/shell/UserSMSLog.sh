#!/bin/sh

# author: tomaer.Ma

# 获取当天日期 & 昨天日期

# 打印出当前日期下的所有的bolt切分过的文件
for file in `hadoop fs -ls /user/storm/hdfs |grep /user/storm |awk '{print $8}'`
do
    echo $file >>~/hdfs.tmp
done
sort ~/hdfs.tmp >>~/hdfs.tmp.sorted
rm -rf ~/hdfs.tmp
sed -i '$d' ~/hdfs.tmp.sorted

for line in `cat ~/hdfs.tmp.sorted`
do
    echo $line
done