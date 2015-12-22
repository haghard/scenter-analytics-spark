import sbt._
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import com.typesafe.sbt.SbtScalariform._
import sbtdocker.ImageName
import scalariform.formatter.preferences._

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(RewriteArrowSymbols, true)
  .setPreference(AlignParameters, true)
  .setPreference(AlignSingleLineCaseStatements, true)

organization := "github.com/haghard"

name := "scenter-analytics-spark"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.7"

val Spark = "1.5.2"
val CassandaConnector = "1.5.0-M3-SNAPSHOT" //with local fixes (SSH was commented)
val CassandraHost = "192.168.0.82"
val akkaStreams = "2.0-M2"

enablePlugins(DockerPlugin)

assemblyJarName in assembly := "scenter-analytics-spark.jar"

parallelExecution in Test := false

mainClass in assembly := Some("http.Bootstrap")

docker <<= (docker dependsOn sbtassembly.AssemblyKeys.assembly)

dockerfile in docker := {
  val jarFile = (assemblyOutputPath in assembly).value
  val appDirPath = "/sport-center"
  val jarTargetPath = s"$appDirPath/${jarFile.name}"

  new Dockerfile {
    from("java:8u45")
    add(jarFile, jarTargetPath)
    workDir(appDirPath)
    entryPoint("java", "-Xmx1256m", "-XX:MaxMetaspaceSize=512m", "-XX:+HeapDumpOnOutOfMemoryError", "-jar", jarTargetPath)
  }
}


promptTheme := ScalapenosTheme

val duplicates = Seq("Absent.class", "Function.class", "Optional$1$1.class", "Optional$1.class", "Optional.class", "Present.class", "Supplier.class")

//http://blog.prabeeshk.com/blog/2014/04/08/creating-uber-jar-for-spark-project-using-sbt-assembly/
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("io", "netty", xs @ _*) => MergeStrategy.first
  case PathList("META-INF", "io.netty.versions.properties", xs @ _*) => MergeStrategy.first
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case "about.html" => MergeStrategy.rename
  case  PathList(ps @ _*) if (duplicates contains ps.last) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter { _.data.getName == "metrics-core-3.0.2.jar" }
}

imageNames in docker := Seq(ImageName(namespace = Some("haghard"),
  repository = "scenter-analytics-spark", tag = Some("v0.1")))

buildOptions in docker := BuildOptions(cache = false,
  removeIntermediateContainers = BuildOptions.Remove.Always,
  pullBaseImage = BuildOptions.Pull.Always)

resolvers ++= Seq(
  "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype" at "https://oss.sonatype.org/content/groups/public/")


libraryDependencies ++= Seq(
  "org.scalaz"              %% "scalaz-core"                    % "7.2.0",
  "com.github.nscala-time"  %% "nscala-time"                    % "2.0.0",

  "org.json4s"              %% "json4s-native"                  % "3.2.10",
  "io.spray"                %% "spray-json"                     % "1.2.6",

  "com.softwaremill.akka-http-session"  %%  "core"              % "0.2.2",   //already has dependencies on akka-http

  "org.mindrot"            %   "jbcrypt"                        % "0.3m",

  "com.haghard"             %% "nosql-join-stream"              % "0.1.5-SNAPSHOT",

  "com.typesafe.akka"       %% "akka-slf4j"                     % "2.4.1",
  "ch.qos.logback"          %  "logback-classic"                % "1.1.2",

  "com.github.scribejava"   %   "scribejava-core"               % "2.0",
  "com.github.scribejava"   %   "scribejava-apis"               % "2.0",
  //"org.twitter4j"           %   "twitter4j-core"                % "4.0.4",
  //"org.scribe"              %   "scribe"                        % "1.3.7",

  ("org.apache.spark"        %% "spark-core"                     % Spark)
    .exclude("javax.xml.bind", "jsr173_api")
    .exclude("org.mortbay.jetty", "servlet-api")
    .exclude("com.google.guava","guava")
    .exclude("org.apache.hadoop","hadoop-yarn-api")
    .exclude("commons-beanutils", "commons-beanutils-core")
    .exclude("commons-collections", "commons-collections")
    .exclude("commons-logging", "commons-logging")
    .exclude("org.spark-project.spark", "unused")
    .exclude("com.esotericsoftware.minlog", "minlog")
    .exclude("org.slf4j", "slf4j-log4j12"),

  ("org.apache.spark"        %% "spark-sql"                      % Spark)
    .exclude("javax.xml.bind", "jsr173_api")
    .exclude("org.mortbay.jetty", "servlet-api")
    .exclude("com.google.guava","guava")
    .exclude("org.apache.hadoop","hadoop-yarn-api")
    .exclude("commons-beanutils", "commons-beanutils-core")
    .exclude("commons-collections", "commons-collections")
    .exclude("commons-logging", "commons-logging")
    .exclude("org.spark-project.spark", "unused")
    .exclude("com.esotericsoftware.minlog", "minlog")
    .exclude("org.slf4j", "slf4j-log4j12"),

  ("org.apache.spark"        %% "spark-streaming"                % Spark)
    .exclude("javax.xml.bind", "jsr173_api")
    .exclude("org.mortbay.jetty", "servlet-api")
    .exclude("com.google.guava","guava")
    .exclude("org.apache.hadoop","hadoop-yarn-api")
    .exclude("commons-beanutils", "commons-beanutils-core")
    .exclude("commons-collections", "commons-collections")
    .exclude("commons-logging", "commons-logging")
    .exclude("org.spark-project.spark", "unused")
    .exclude("com.esotericsoftware.minlog", "minlog")
    .exclude("org.slf4j", "slf4j-log4j12"),

  ("com.datastax.spark"      %% "spark-cassandra-connector"      % CassandaConnector)
    .exclude("javax.xml.bind", "jsr173_api")
    .exclude("org.mortbay.jetty", "servlet-api")
    .exclude("com.google.guava","guava")
    .exclude("org.apache.hadoop","hadoop-yarn-api")
    .exclude("commons-beanutils", "commons-beanutils-core")
    .exclude("commons-collections", "commons-collections")
    .exclude("commons-logging", "commons-logging")
    .exclude("org.spark-project.spark", "unused")
    .exclude("com.esotericsoftware.minlog", "minlog")
    .exclude("org.slf4j", "slf4j-log4j12"),

  "org.scalatest"           %% "scalatest"                      % "2.2.5"          %   "test"
)

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-target:jvm-1.7",
  "-deprecation",
  "-unchecked",
  "-Ywarn-dead-code",
  "-feature",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-language:existentials")

javacOptions ++= Seq(
  "-source", "1.7",
  "-target", "1.7",
  "-Xlint:unchecked",
  "-Xlint:deprecation")

addCommandAlias("lanalytics", s"run-main  http.Bootstrap --HTTP_PORT=8001 --NET_INTERFACE=en0 --DB_HOSTS=$CassandraHost")


//http GET http://192.168.0.62:8001/api/login?"user=haghard&password=qwerty"
//browser http GET http://192.168.0.62:8001/api/twitter-login"

//http GET http://192.168.0.62:8001/api/standing/season-15-16 'Cookie:_sessiondata= ...'
//http GET http://192.168.0.62:8001/api/standing/playoff-14-15 'Cookie:_sessiondata= ...'

//http GET http://192.168.0.62:8001/api/teams/season-15-16?teams=cle,okc 'Cookie:_sessiondata=...'

//http GET http://192.168.0.62:8001/api/player/stats?"name=S. Curry&period=season-15-16&team=gsw"

//http GET http://192.168.0.62:8001/api/leaders/pts/season-14-15
//http GET http://192.168.0.62:8001/api/leaders/reb/season-15-16

//??http GET http://192.168.0.62:8001/api/results/search?"from=2015-10-26&to=2015-11-03&teams=okc,cle"

//docker

//docker run --net="host" -d haghard/scenter-analytics-spark:v0.1 --HTTP_PORT=8001 --DB_HOSTS=192.168.0.xxx