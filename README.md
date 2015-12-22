SCenter analytics
================
Analytics application which doesn't part of the cluster from [Sport Center](https://github.com/haghard/sport-center) but allows run Spark jobs using Cassandra as a source of data

Run in local mode from sbt
=================

`sbt lanalytics`


Get docker image
=================

`docker pull haghard/scenter-analytics-spark:v0.1`


Run docker image in interactive mode
================

`docker run --net="host" -it haghard/scenter-analytics-spark:v0.1 --HTTP_PORT=8001 --DB_HOSTS=192.168.0.xxx` 


By default server is going to bind on "eth0" enterface. If you need to change it you should pass additional parameter --NET_INTERFACE

`docker run --net="host" -it haghard/scenter-analytics-spark:v0.1 --HTTP_PORT=8001 --DB_HOSTS=192.168.0.xxx --NET_INTERFACE=en0`


Example routes
===============

Through browser http://192.168.0.62:8001/api/twitter-login"

http GET [host]:[port]/api/login?"user=...&password=..."

http GET [host]:[port]/api/standing/playoff-14-15?teams=cle,okc 'Cookie:_sessiondata=...'

http GET [host]:[port]/api/teams/season-15-16?teams=cle,okc 'Cookie:_sessiondata=...'

http GET [host]:[port]/api/player/stats?"name=S. Curry&period=season-15-16&team=gsw"

http GET [host]:[port]/api/leaders/pts/season-14-15?teams=cle,okc 'Cookie:_sessiondata=...' 

http GET [host]:[port]/api/leaders/reb/season-15-16?teams=cle,okc 'Cookie:_sessiondata=...'