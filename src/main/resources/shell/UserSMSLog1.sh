#!/bin/sh

# author: tomaer.Ma

# 获取当天日期 & 昨天日期
DATE=`date "+%Y-%m-%d"`
YESTERDAY=`date -d "1 day ago" +"%Y-%m-%d"`
hadoop fs -rm -R /tmp/storm/UserSMSLog/${DATE}
hadoop fs -mkdir -p /tmp/storm/UserSMSLog/${DATE}

# hadoop fs -chown hdfs:hadoop /tmp/storm/UserSMSLog/${DATE}
# hadoop fs -chmod 777 /tmp/storm/UserSMSLog/${DATE}

# 打印出当前日期下的所有的bolt切分过的文件
for file in `hadoop fs -ls /user/storm/UserSMSLog/${DATE} |grep /user/storm |awk '{print $5"_"$8}'`
do
    filename=${file##*/}
    hadoop fs -cp $file /tmp/storm/UserSMSLog/${DATE}/${filename}
    hive -e "LOAD DATA INPATH '/tmp/storm/UserSMSLog/${DATE}/${filename}' into table storm.usersmslog partition (dt='${DATE}')"
done