#!/bin/sh

# author: tomaer.Ma

# 文件夹是从外部传递进来的
export FOLDER=$1

# 获取当天日期
DATE=`date "+%Y-%m-%d"`

# 创建指定的文件夹
hadoop fs -mkdir -p /user/storm/${FOLDER}/${DATE}
hadoop fs -chown hdfs:hadoop /user/storm/${FOLDER}/${DATE}
hadoop fs -chmod 777 /user/storm/${FOLDER}/${DATE}


# 获取 hdfs 目录下面所有的文件,循环将文件写入到文本文件中
for file in `hadoop fs -ls /user/storm/${FOLDER} |grep /user/storm |awk '{print $8}'`
do
    echo $file >>/home/hdfs/${FOLDER}.file
done

# 对文本文件的内容进行排序,按文件名上的时间戳 asc
sort /home/hdfs/${FOLDER}.file >>/home/hdfs/${FOLDER}.needmv
rm -rf /home/hdfs/${FOLDER}.file

# 读取文本文件最后一行数据,即需要使用 cp 命令进行操作的文件
LASTLINE=$(tail -1 /home/hdfs/${FOLDER}.needmv)

# 删除文本文件的最后一行数据
sed -i '$d' /home/hdfs/${FOLDER}.needmv

# 循环 mv 文件到指定的目录
for line in `cat /home/hdfs/${FOLDER}.needmv`
do
    mvFilename=${line##*/}
    hadoop fs -mv $line /user/storm/${FOLDER}/${DATE}/${mvFilename}
done
rm -rf /home/hdfs/${FOLDER}.needmv

# cp 文件到指定的目录
cpFilename=${LASTLINE##*/}
hadoop fs -cp $LASTLINE /user/storm/${FOLDER}/${DATE}/${cpFilename}