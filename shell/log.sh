#!/bin/bash

case $1 in
"start")
    for host in hadoop102 hadoop103 hadoop104 ; do
        echo "$host 启动日志服务器"
        ssh $host "source /etc/profile ; nohup java -jar /opt/gmall0213/gmall-logger-0.0.1-SNAPSHOT.jar 1>/dev/null 2>&1 &"
    done
   ;;
"stop")
    for host in hadoop102 hadoop103 hadoop104 ; do
        echo "$host 停止日志服务器"
        ssh $host "source /etc/profile ; jps | grep gmall-logger-0.0.1-SNAPSHOT.jar | awk '{print \$1}' | xargs kill -9"
    done
   ;;
*)
    echo '启动姿势不对'
    echo ' log.sh start 启动日志采集服务器'
    echo ' log.sh stop  停止日志采集服务器'
   ;;
esac



