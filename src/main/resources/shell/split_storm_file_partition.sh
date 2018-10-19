#!/bin/sh
# author: tomaer.Ma

# 获取当天日期
DATE=`date "+%Y%m%d"`
DATE2=`date "+%Y-%m-%d"`

hadoop fs -mkdir -p /user/storm/UserSMSLog/${DATE2}
hadoop fs -mkdir -p /user/storm/BuryPoint/${DATE2}
hadoop fs -chown -R hdfs:hadoop /user/storm/UserSMSLog/${DATE2}
hadoop fs -chmod -R 777 /user/storm/UserSMSLog/${DATE2}
hadoop fs -chown -R hdfs:hadoop /user/storm/BuryPoint/${DATE2}
hadoop fs -chmod -R 777 /user/storm/BuryPoint/${DATE2}

echo "create table tmp.usersmslog_${DATE} as select * from tmp.usersmslog where from_unixtime(unix_timestamp(createdon)+28800) between '${DATE2} 00:00:00' and '${DATE2} 23:59:59' order by id;" >>/home/hdfs/partition_${DATE}
echo "create table tmp.loginburypoint_${DATE} as select * from tmp.loginburypoint where from_unixtime(unix_timestamp(createdon)+28800) between '${DATE2} 00:00:00' and '${DATE2} 23:59:59' order by createdon;" >>/home/hdfs/partition_${DATE}

hive -f /home/hdfs/partition_${DATE}
rm -rf /home/hdfs/partition_${DATE}

for file in `hadoop fs -ls /apps/hive/warehouse/tmp.db/usersmslog_${DATE} |grep /apps/hive/warehouse/tmp.db |awk '{print $8}'`
do
    echo "LOAD DATA INPATH '${file}' into table usersmslog partition (dt='${DATE2}');" >>/home/hdfs/partition_${DATE}
done

for file in `hadoop fs -ls /apps/hive/warehouse/tmp.db/loginburypoint_${DATE} |grep /apps/hive/warehouse/tmp.db |awk '{print $8}'`
do
    echo "LOAD DATA INPATH '${file}' into table loginburypoint partition (dt='${DATE2}');" >>/home/hdfs/partition_${DATE}
done

echo "drop table tmp.usersmslog_${DATE};" >>/home/hdfs/partition_${DATE}
echo "drop table tmp.loginburypoint_${DATE};" >>/home/hdfs/partition_${DATE}

hive -f /home/hdfs/partition_${DATE}
rm -rf /home/hdfs/partition_${DATE}

# 移除 UserSMSLog 文件
for file in `hadoop fs -ls /tmp/storm/UserSMSLog |grep /tmp/storm |awk '{print $8}'`
do
    echo $file >>/home/hdfs/partition_${DATE}
done
sort /home/hdfs/partition_${DATE} >>/home/hdfs/partition_${DATE}.needmv
rm -rf /home/hdfs/partition_${DATE}
sed -i '$d' /home/hdfs/partition_${DATE}.needmv

# 循环 mv 文件到指定的目录
for line in `cat /home/hdfs/partition_${DATE}.needmv`
do
    mvFilename=${line##*/}
    hadoop fs -mv $line /user/storm/UserSMSLog/${DATE2}/${mvFilename}
done
rm -rf /home/hdfs/partition_${DATE}.needmv

# 移除 BuryPoint 文件
for file in `hadoop fs -ls /tmp/storm/BuryPoint |grep /tmp/storm |awk '{print $8}'`
do
    echo $file >>/home/hdfs/partition_${DATE}
done
sort /home/hdfs/partition_${DATE} >>/home/hdfs/partition_${DATE}.needmv
rm -rf /home/hdfs/partition_${DATE}
sed -i '$d' /home/hdfs/partition_${DATE}.needmv

# 循环 mv 文件到指定的目录
for line in `cat /home/hdfs/partition_${DATE}.needmv`
do
    mvFilename=${line##*/}
    hadoop fs -mv $line /user/storm/BuryPoint/${DATE2}/${mvFilename}
done
rm -rf /home/hdfs/partition_${DATE}.needmv

echo "successfully"