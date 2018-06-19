#!/bin/bash
#echo "Input the neo4j home_dir first:"
#read home_dir
source /etc/profile
home_dir=/home/neo4j/neo4j-community-3.3.1_b
db_file=$home_dir/data/databases/graph.db
#shell_dir="$( cd -P "$( dirname "$SOURCE"  )" && pwd  )"
log_path=/home/neo4j/shellCmdLogs
log_file=/home/neo4j/shellCmdLogs/importTotalData_b.log             #记录shell命令的log文件绝对路径

#判断log_path和log_file是否存在，不存在的话自动创建新的
if [ ! -d "$log_path" ];then
   mkdir -p $log_path
   echo "`date "+%Y-%m-%d %H:%M:%S"` $@: create $log_path success!"
fi 
if [ ! -f "$log_file" ];then
   touch $log_file
   chmod 755 $log_file
   echo "`date "+%Y-%m-%d %H:%M:%S"` $@: create $log_file success!"
fi

#判断neo4j进程的PID是否存在，存在则杀死
PID=$(ps aux | grep "$home_dir" | grep -v grep | awk '{print $2}')

if [ $? -eq 0 ]; then
    echo "`date "+%Y-%m-%d %H:%M:%S"` $@: neo4j process id:$PID" &>>$log_file
else
    echo "`date "+%Y-%m-%d %H:%M:%S"` $@: process neo4j not exit" &>>$log_file
    exit
fi

kill -9 ${PID}

if [ $? -eq 0 ];then
    echo "`date "+%Y-%m-%d %H:%M:%S"` $@: kill neo4j success!" &>>$log_file
else
    echo "`date "+%Y-%m-%d %H:%M:%S"` $@: kill neo4j fail! Maybe it have been shut down" &>>$log_file
fi

#判断/data/databases/graph.db文件是否存在，存在则删除
cd $home_dir 
if [ ! -d "$db_file" ];then
echo "`date "+%Y-%m-%d %H:%M:%S"` $@: graph.db is not exist!" &>>$log_file
else
rm -rf $db_file
echo "`date "+%Y-%m-%d %H:%M:%S"` $@: graph.db has been deleted!" &>>$log_file
fi

#全量导入数据
cd $home_dir/bin
./neo4j-admin import --database=graph.db --mode=csv --nodes:company="$home_dir/import/companies.csv" --nodes:company="$home_dir/import/extraNodes.csv" --relationships:invests="$home_dir/import/relations_invest.csv" --relationships:guarantees="$home_dir/import/relations_guarantee.csv" --id-type=INTEGER --ignore-duplicate-nodes=true --ignore-extra-columns=true --ignore-missing-nodes=true --multiline-fields=true
echo "`date "+%Y-%m-%d %H:%M:%S"` $@: import total data successfully!" &>>$log_file  

#启动neo4j Server
#ulimit -n 65536
source /etc/profile
./neo4j start
#判断neo4j start是否成功
PID_1=$(ps aux | grep "$home_dir" | grep -v grep | awk '{print $2}')
if [ $? -eq 0 ]; then
    echo "`date "+%Y-%m-%d %H:%M:%S"` $@: execute start cmd first time, neo4j is start up! neo4j process id:$PID_1"  &>>$log_file
else
    echo "`date "+%Y-%m-%d %H:%M:%S"` $@: first time start neo4j fail!" &>>$log_file
    #再次执行./neo4j start命令
    ./neo4j start
    PID_2=$(ps aux | grep "$home_dir" | grep -v grep | awk '{print $2}')
    if [ $? -eq 0 ]; then
        echo "`date "+%Y-%m-%d %H:%M:%S"` $@: execute start cmd second time, neo4j is start up! neo4j process id:$PID_2"  &>>$log_file
    else
        echo "`date "+%Y-%m-%d %H:%M:%S"` $@: second time start neo4j fail!" &>>$log_file
        exit
fi
