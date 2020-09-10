#!/bin/bash
JAVA_BIN=/opt/module/jdk1.8.0_144/bin/java
#PROJECT= spark-ck-es-mock-log/logger_server
APPNAME=gmall-logger-0.0.1-SNAPSHOT.jar
SERVER_PORT=8080

case $1 in
 "start")
   {

    for i in hadoop102 hadoop103 hadoop104
    do
     echo "========: $i==============="
    ssh $i  "$JAVA_BIN -Xms32m -Xmx64m  -jar /home/atguigu/spark-ck-es-mock-log/logger_server/$APPNAME --server.port=$SERVER_PORT >/dev/null 2>&1  &"
    done
     echo "========NGINX==============="
    /opt/module/nginx_spark_ch_es_realtime/sbin/nginx
  };;
  "stop")
  {
     echo "======== NGINX==============="
    /opt/module/nginx_spark_ch_es_realtime/sbin/nginx  -s stop
    for i in hadoop102 hadoop103 hadoop104
    do
     echo "========: $i==============="
     ssh $i "ps -ef|grep $APPNAME |grep -v grep|awk '{print \$2}'|xargs kill" >/dev/null 2>&1
    done

  };;
   esac