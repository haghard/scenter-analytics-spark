SCenter analytics
================
Analytics application which doesn't part of the cluster from [Sport Center](https://github.com/haghard/sport-center) but allows run Spark jobs using Cassandra as a source of data

Run in local mode from sbt
=================

`sbt lanalytics`


Get docker image
=================

`docker pull haghard/scenter-spark-analytics:v0.3`


Run with docker
================

`docker run -d -p 8080:8080 haghard/scenter-spark-analytics:v0.3 --HTTP_PORT=8080 --DB_HOSTS=109.234.39.32 --DOMAIN=192.168.0.182 --TWITTER_CONSUMER_KEY=<...> --TWITTER_CONSUMER_SECRET=<...> --GOOGLE_CONSUMER_KEY=<...> --GOOGLE_CONSUMER_SECRET=<...> --GITHUB_CONSUMER_KEY=<...> --GITHUB_CONSUMER_SECRET=<...>` 


Example routes
===============

Open browser to get Authorization url

http://[host]:[port]/api/login-twitter

http://[host]:[port]/api/login-google

http://[host]:[port]/api/login-github


http GET [host]:[port]/api/login?"user=...&password=..."

http GET [host]:[port]/api/standing/playoff-14-15?"teams=cle,okc" Authorization:...

http GET [host]:[port]/api/teams/season-15-16?"teams=cle,okc" Authorization:...

http GET [host]:[port]/api/player/stats?"name=S. Curry&period=season-15-16&team=gsw" Authorization:...

http GET [host]:[port]/api/leaders/pts/season-15-16 Authorization:... 

http GET [host]:[port]/api/leaders/reb/season-15-16 Authorization:...

http GET [host]:[port]/api/daily/2015-01-16 Authorization:...

### Useful links 
 
https://www.digitalocean.com/community/tutorials/how-to-set-up-a-host-name-with-digitalocean
http://blog.prabeeshk.com/blog/2014/04/08/creating-uber-jar-for-spark-project-using-sbt-assembly/
http://www.sestevez.com/sestevez/CassandraDataModeler/
https://blog.cloudera.com/blog/2016/06/how-to-analyze-fantasy-sports-using-apache-spark-and-sql/



run-main http.Bootstrap --HTTP_PORT=8001 --NET_INTERFACE=en0 --DB_HOSTS=192.168.0.182,192.168.0.38 --DOMAIN=192.168.0.62  --TWITTER_CONSUMER_KEY=zhmdNNiYTZq8kOkn8g9vrEUkp --TWITTER_CONSUMER_SECRET=IzqrKp0ksZoEhsxCw9Tbk4WutvVJNZBg6CZYKNjLUMonOSbVXx --GOOGLE_CONSUMER_KEY=460103596995-7o4m205tisva5b6bolooc6flm1udpr5t.apps.googleusercontent.com --GOOGLE_CONSUMER_SECRET=x2Yq7bY1ebpXkJCskmSHLO4b --GITHUB_CONSUMER_KEY=d7f8218f3c94e7a11f82 --GITHUB_CONSUMER_SECRET=ed68e09ad0f613f599c909df50fff2d99ab88c1e