import java.util.Date

import akka.event.LoggingAdapter
import cassandra._
import com.typesafe.config.Config
import http.NbaResult
import http.SparkJob._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.{ SparkConf, SparkContext }

import scala.annotation.implicitNotFound
import scala.collection.mutable
import scala.concurrent.{ ExecutionContext, Future }

package object spark {

  trait SparkQuery {
    def name: String

    def createSparkContext(config: Config, cassandraHost: String) =
      new SparkContext(
        new SparkConf()
          .setAppName(name)
          .set("spark.cassandra.connection.host", cassandraHost)
          .set("spark.cassandra.connection.timeout_ms", "8000")
          .set("spark.cleaner.ttl", "3600")
          .set("spark.eventLog.dir", "spark-logs")
          .set("spark.akka.frameSize", "50")
          .set("spark.default.parallelism", "4")
          .set("spark.streaming.backpressure.enabled", "true")
          .set("spark.streaming.backpressure.pid.minRate", "1000")
          .setMaster(config.getString("spark.master"))
      )
  }

  trait StandingQuery[T] extends SparkQuery {
    def async(
      ctx: SparkContext,
      config: Config,
      teams: mutable.HashMap[String, String],
      period: String
    ): Future[T]
  }

  trait PtsLeadersQuery[T] extends SparkQuery {
    def async(
      ctx: SparkContext,
      config: Config,
      period: String,
      depth: Int
    ): Future[T]
  }

  trait RebLeadersQuery[T] extends SparkQuery {
    def async(
      ctx: SparkContext,
      config: Config,
      period: String,
      depth: Int
    ): Future[T]
  }

  trait PlayerStatsQuery[T] extends SparkQuery {
    def async(
      ctx: SparkContext,
      config: Config,
      name: String,
      period: String,
      team: String
    ): Future[T]
  }

  trait TeamsResultsQuery[T] extends SparkQuery {
    def async(ctx: SparkContext, config: Config, period: String, teams: scala.collection.Seq[String],
      arenas: Seq[(String, String)], allTeams: mutable.HashMap[String, String]): Future[T]
  }

  trait DailyResultsQuery[T] extends SparkQuery {
    def async(
      ctx: SparkContext,
      config: Config,
      stage: String,
      yyyyMMDD: (Int, Int, Int),
      arenas: Seq[(String, String)],
      allTeams: mutable.HashMap[String, String]
    ): Future[T]
  }

  object DailyResultsQuery {
    @implicitNotFound(msg = "Cannot find DailyResultsQuery type class for ${T}")
    def apply[T <: SparkQueryView: DailyResultsQuery](implicit ex: ExecutionContext) = implicitly[DailyResultsQuery[T]]

    implicit def teamStats(implicit ex: ExecutionContext) =
      new DailyResultsQuery[DailyView] {
        override val name: String = "[spark-query]: daily-results"

        override def async(
          ctx: SparkContext,
          config: Config,
          stage: String,
          yyyyMMDD: (Int, Int, Int),
          arenas: Seq[(String, String)],
          teams: mutable.HashMap[String, String]
        ): Future[DailyView] = {
          val startTs = System.currentTimeMillis()
          val results = ctx.cassandraDailyResults(config, stage, yyyyMMDD._1, yyyyMMDD._2, yyyyMMDD._3).cache()
          results.collectAsync().map { seq =>
            val results = seq.map { el =>
              ResultView(s" ${el._2} @ ${el._1}", s"${el._7}:${el._5} - ${el._6}:${el._4}", cassandra.formatter.format(el._3),
                arenas.find(_._1 == el._1.trim).map(_._2).getOrElse(el._1))
            }
            DailyView(results.size, results.toList, System.currentTimeMillis - startTs)
          }
        }
      }
  }

  object TeamsResultsQuery {

    @implicitNotFound(msg = "Cannot find TeamsResultsQuery type class for ${T}")
    def apply[T <: SparkQueryView: TeamsResultsQuery](implicit ex: ExecutionContext) = implicitly[TeamsResultsQuery[T]]

    implicit def teamStats(implicit ex: ExecutionContext) =
      new TeamsResultsQuery[TeamStatsView] {
        override val name: String = "[spark-query]: team-stats"
        override def async(ctx: SparkContext, config: Config, period: String, teams: scala.collection.Seq[String], arenas: Seq[(String, String)],
          allTeams: mutable.HashMap[String, String]): Future[TeamStatsView] = {
          val sqlContext = new SQLContext(ctx)
          import sqlContext.implicits._
          val startTs = System.currentTimeMillis

          val keyTeamsDF = ctx.parallelize(teams).toDF("team")
          val arenasDF = ctx.parallelize(arenas).toDF("home-team", "arena")

          val resultsDF = ctx.cassandraResultByPeriodRdd(config, allTeams.keySet, period).toDF("home-team", "home-score", "away-team", "away-score", "date")

          ((keyTeamsDF join (resultsDF, resultsDF("home-team") === keyTeamsDF("team") || resultsDF("away-team") === keyTeamsDF("team"))) join (arenasDF, "home-team"))
            .sort(resultsDF("date"))
            .map { row: Row ⇒
              ResultView(s"${row.getAs[String]("away-team")} @ ${row.getAs[String]("home-team")}",
                s"${row.getAs[Int]("away-score")} : ${row.getAs[Int]("home-score")}",
                cassandra.formatter.format(row.getAs[java.sql.Date]("date")), row.getAs[String]("arena"))
            }.collectAsync()
            .map(res ⇒ TeamStatsView(res.size, res.toList, System.currentTimeMillis - startTs))
        }
      }
  }

  object PlayerStatsQuery {

    @implicitNotFound(msg = "Cannot find PlayerStatsQuery type class for ${T}")
    def apply[T <: SparkQueryView: PlayerStatsQuery](implicit ex: ExecutionContext) = implicitly[PlayerStatsQuery[T]]

    /*
    implicit def avg(implicit ex: ExecutionContext) = new PlayerStatsTask[PlayerStatsView] {
      override val name: String = "[spark-task]: player-avg"

      override def async(ctx: SparkContext, config: Config, name: String, period: String, team: String): Future[PlayerStatsView] = {
        val start = System.currentTimeMillis()
        val rdd: RDD[(String, Date, String, Int, Int, Int, String, String, String, Int, Int, Int, String, Int)] =
          ctx.cassandraPlayerRdd(config, name, period, team).cache()

        rdd

        rdd.collectAsync().map { batch ⇒
          val st = batch.map(r ⇒ Stats(r._1, r._2, r._3, r._4, r._5, r._6, r._7, r._8, r._9, r._10, r._11, r._12, r._13, r._14)).toList
          PlayerStatsView(st.length, stats = st, System.currentTimeMillis() - start)
        }
      }
    }*/

    implicit def stats(implicit ex: ExecutionContext) =
      new PlayerStatsQuery[PlayerStatsView] {
        override val name: String = "[spark-query]: player-stats"

        override def async(ctx: SparkContext, config: Config,
          playerName: String, period: String, team: String): Future[PlayerStatsView] = {
          val start = System.currentTimeMillis()
          val rdd: RDD[(String, Date, String, Int, Int, Int, String, String, String, Int, Int, Int, String, Int)] =
            ctx.cassandraPlayerRdd(config, playerName, period, team).cache()

          rdd.collectAsync().map { batch ⇒
            val st = batch
              .map(r ⇒ Stats(r._1, r._2, r._3, r._4, r._5, r._6, r._7, r._8, r._9, r._10, r._11, r._12, r._13, r._14))
              .toList
            PlayerStatsView(st.length, stats = st, System.currentTimeMillis() - start)
          }
        }
      }
  }

  object RebLeadersQuery {

    @implicitNotFound(msg = "Cannot find RebLeadersTask type class for ${T}")
    def apply[T <: SparkQueryView: RebLeadersQuery](implicit ex: ExecutionContext) = implicitly[RebLeadersQuery[T]]

    implicit def instance(implicit ex: ExecutionContext) =
      new RebLeadersQuery[RebLeadersView] {
        override val name = "[spark-query]: rebound-leaders"

        override def async(ctx: SparkContext, config: Config, period: String, depth: Int): Future[RebLeadersView] = {
          val start = System.currentTimeMillis()
          val rdd: RDD[((String, String), (Float, Float, Float))] = ctx.cassandraRebLeadersRdd(config, period).cache()

          val array = rdd
            .combineByKey(
              (tuple: (Float, Float, Float)) ⇒ (tuple, 1l),
              (res: ((Float, Float, Float), Long),
                tuple: (Float, Float, Float)) ⇒
                ((res._1._1 + tuple._1,
                  res._1._2 + tuple._2,
                  res._1._3 + tuple._3),
                  res._2 + 1),
              (left: ((Float, Float, Float), Long),
                right: ((Float, Float, Float), Long)) ⇒
                ((left._1._1 + right._1._1,
                  left._1._2 + right._1._2,
                  left._1._3 + right._1._3),
                  left._2 + right._2)
            )
            .mapValues({
              case ((offReb, defReb, totalReb), count) ⇒
                if (count > 0)
                  ((
                    (offReb / count).toDouble,
                    (defReb / count).toDouble,
                    (totalReb / count).toDouble
                  ),
                    count)
                else ((0.0, 0.0, 0.0), count)
            })
            .sortBy(_._2._1._3, ascending = false)
            .take(depth)

          Future {
            RebLeadersView(
              array.length,
              array
              .map(
                kv ⇒
                  RebLeader(
                    team = kv._1._2,
                    player = kv._1._1,
                    offensive = kv._2._1._1.scale(2),
                    defensive = kv._2._1._2.scale(2),
                    total = kv._2._1._3.scale(2),
                    games = kv._2._2
                  )
              )
              .toList,
              System.currentTimeMillis() - start
            )
          }
        }
      }
  }

  object PtsLeadersQuery {

    @implicitNotFound(msg = "Cannot find PtsLeadersTask type class for ${T}")
    def apply[T <: SparkQueryView: PtsLeadersQuery](
      implicit
      ex: ExecutionContext
    ) = implicitly[PtsLeadersQuery[T]]

    implicit def ptsLeaders(implicit ex: ExecutionContext) =
      new PtsLeadersQuery[PtsLeadersView] {
        override val name = "[spark-task]: pts-leaders"

        /*
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val df = rdd.toDF("player", "team", "pts")
        val f = df.groupBy(df("player"), df("team"))
          .agg(df("player"), df("team"), avg(df("pts")).as("avgp"), count(df("pts")).as("g"))
          .sort($"avgp".desc)
          .limit(depth)
          .map { row ⇒ PtsLeader(row.getAs[String]("team"), row.getAs[String]("player"), row.getAs[Double]("avgp").scale(2), row.getAs[Long]("g")) }
          .collectAsync
          .map { leaders ⇒ PtsLeadersView(leaders.size, leaders.toList, System.currentTimeMillis() - sc.startTime) }
         */
        override def async(
          ctx: SparkContext,
          config: Config,
          period: String,
          depth: Int
        ): Future[PtsLeadersView] = {
          val start = System.currentTimeMillis()
          val rdd: RDD[((String, String), Float)] =
            ctx.cassandraPtsLeadersRdd(config, period).cache()

          //aggregation
          val array = rdd
            .combineByKey(
              (pts: Float) ⇒ (pts, 1l),
              (res: (Float, Long), pts: Float) ⇒ (res._1 + pts, res._2 + 1),
              (res1: (Float, Long),
                res2: (Float, Long)) ⇒ (res1._1 + res2._1, res1._2 + res2._2)
            )
            .mapValues({
              case (sum, count) ⇒
                if (count > 0) ((sum / count).toDouble, count)
                else (0.0, count)
            })
            .sortBy(_._2._1, ascending = false)
            .take(depth)

          Future {
            PtsLeadersView(
              array.length,
              array
              .map(
                kv ⇒
                  PtsLeader(
                    team = kv._1._2,
                    player = kv._1._1,
                    pts = kv._2._1.scale(2),
                    games = kv._2._2
                  )
              )
              .toList,
              System.currentTimeMillis() - start
            )
          }
        }
      }
  }

  object StandingQuery {

    import scala.collection.immutable.TreeMap
    import scala.collection.mutable

    val first = List.range(1, 9).map(_ + ". First round")
    val second = List.range(1, 5).map(_ + ". Conference semifinal")
    val semifinal = List.range(1, 3).map(_ + ". Conference final")
    val stageNames = first ::: second ::: semifinal ::: List("Final. ")

    @implicitNotFound(
      msg = "Cannot find CassandraStandingTask type class for ${T}"
    )
    def apply[T <: SparkQueryView: StandingQuery](
      implicit
      ex: ExecutionContext
    ) = implicitly[StandingQuery[T]]

    implicit def playoff(implicit ex: ExecutionContext) =
      new StandingQuery[PlayoffStandingView] {
        override val name = "[spark-query]: standing-playoff"

        override def async(
          ctx: SparkContext,
          config: Config,
          teams: mutable.HashMap[String, String],
          period: String
        ): Future[PlayoffStandingView] = {
          val start = System.currentTimeMillis()
          val rdd: RDD[NbaResult] =
            ctx.cassandraStandingRdd(config, teams.keySet, period).cache()

          /*
        rdd.keyBy(v ⇒ Set(v.homeTeam, v.awayTeam))
          .combineByKey(
            (r: NbaResult) ⇒ {
              if (r.homeScore > r.awayScore) TreeMap[Date, String](r.dt -> s"[${r.homeScore}:${r.awayScore} ${r.homeTeam} won]")
              else TreeMap[Date, String](r.dt -> s"[${r.homeScore}:${r.awayScore} ${r.awayTeam} won]")
            },
            (res: TreeMap[Date, String], pts: NbaResult) ⇒
              if (pts.homeScore > pts.awayScore) res + (pts.dt -> s"[${pts.homeScore}:${pts.awayScore} ${pts.homeTeam} won]")
              else res + (pts.dt -> s"[${pts.homeScore}:${pts.awayScore} ${pts.awayTeam} won]")
            ,
            (res1: TreeMap[Date, String], res2: TreeMap[Date, String]) ⇒ res1 ++ res1
          )
          .map(kv ⇒ (kv._2.lastKey, kv._1.head + "-" + kv._1.last + "  " + kv._2.values.mkString(", ")))
          .sortByKey()
          .map(_._2)
          .collectAsync()
          .map { data ⇒
            val r = (stageNames zip data).map(kv ⇒ s"${kv._1} ${kv._2}")
            PlayoffStandingView(r.size, r.toList, System.currentTimeMillis() - start)
          }
           */

          rdd
            .keyBy(v ⇒ Set(v.homeTeam, v.awayTeam))
            .aggregateByKey(TreeMap[Date, String]())(
              (key, r) ⇒
                if (r.homeScore > r.awayScore)
                  key + (r.dt -> s"[${r.homeScore}:${r.awayScore} ${r.homeTeam} won]")
                else
                  key + (r.dt -> s"[${r.homeScore}:${r.awayScore} ${r.awayTeam} won]"),
              (m0, m1) ⇒ m0 ++ m1
            )
            .map { kv ⇒
              (kv._2.lastKey,
                kv._1.head + "-" + kv._1.last + "  " + kv._2.values.mkString(
                  ", "
                ))
            }
            .sortByKey()
            .map(_._2)
            .collectAsync()
            .map { rdd ⇒
              val r = (stageNames zip rdd).map(kv ⇒ s"${kv._1} ${kv._2}")
              PlayoffStandingView(
                r.size,
                r.toList,
                System.currentTimeMillis() - start
              )
            }
        }
      }

    implicit def season(implicit ex: ExecutionContext) =
      new StandingQuery[SeasonStandingView] {
        override val name = "[spark-query]: season-standing"

        override def async(
          ctx: SparkContext,
          config: Config,
          teams: mutable.HashMap[String, String],
          period: String
        ): Future[SeasonStandingView] = {
          val start = System.currentTimeMillis
          val rdd: RDD[NbaResult] =
            ctx.cassandraStandingRdd(config, teams.keySet, period).cache()

          rdd.flatMap { r ⇒
            if (r.homeScore > r.awayScore)
              List((r.homeTeam, "hw"), (r.awayTeam, "al"))
            else List((r.homeTeam, "hl"), (r.awayTeam, "aw"))
          }.aggregateByKey(Standing())(
            (acc: Standing, state: String) ⇒
              state match {
                case "hw" ⇒ acc.copy(hw = acc.hw + 1, w = acc.w + 1)
                case "hl" ⇒ acc.copy(hl = acc.hl + 1, l = acc.l + 1)
                case "aw" ⇒ acc.copy(aw = acc.aw + 1, w = acc.w + 1)
                case "al" ⇒ acc.copy(al = acc.al + 1, l = acc.l + 1)
              }, { (f, s) ⇒
              f.copy(
                f.team,
                f.hw + s.hw,
                f.hl + s.hl,
                f.aw + s.aw,
                f.al + s.al,
                f.w + s.w,
                f.l + s.l
              )
            }
          )
            .map(kv ⇒ kv._2.copy(team = kv._1))
            .sortBy(_.w, ascending = false)
            .collectAsync()
            .map { seq ⇒
              val (west, east) = seq.partition { item ⇒
                teams(item.team) match {
                  case "west" ⇒ true
                  case "east" ⇒ false
                }
              }
              SeasonStandingView(
                west.size + east.size,
                west.toList,
                east.toList,
                System.currentTimeMillis - start
              )
            }
        }
      }
  }

  /*
  object StandingJournalTask {
    import scala.collection.immutable.TreeMap
    import scala.collection.mutable

    val first = List.range(1, 9).map(_ + ". First round")
    val second = List.range(1, 5).map(_ + ". Conference semifinal")
    val semifinal = List.range(1, 3).map(_ + ". Conference final")
    val stageNames = first ::: second ::: semifinal ::: List("Final. ")

    @implicitNotFound(msg = "Cannot find StandingJournalTask type class for ${T}")
    def apply[T <: SparkJobView: StandingJournalTask](implicit ex: ExecutionContext) = implicitly[StandingJournalTask[T]]

    implicit def playoff(implicit ex: ExecutionContext) = new StandingJournalTask[PlayoffStandingView] {
      override val name = "[spark-task]: PlayoffStandingView"
      override def async(ctx: SparkContext, config: Config, teams: mutable.HashMap[String, String], interval: Interval): Future[PlayoffStandingView] = {
        val start = System.currentTimeMillis
        val rdd: RDD[(String, ByteBuffer)] = ctx.cassandraJournalRdd(config, teams.keySet).cache()

        rdd.mapValues(deserialize(_))
          .collect {
            case (team, event) if interval contains new DateTime(event.getResult.getTime).withZone(SCENTER_TIME_ZONE) ⇒
              (Set(event.getResult.getHomeTeam, event.getResult.getAwayTeam),
                NbaResult(event.getResult.getHomeTeam, event.getResult.getHomeScore, event.getResult.getAwayTeam, event.getResult.getAwayScore,
                  new Date(event.getResult.getTime)))
          }.aggregateByKey(TreeMap[Date, String]())((round, r) ⇒
            if (r.homeScore > r.awayScore) round + (r.dt -> s"[${r.homeScore}:${r.awayScore} ${r.homeTeam} won]")
            else round + (r.dt -> s"[${r.homeScore}:${r.awayScore} ${r.awayTeam} won]"),
            (m0, m1) ⇒ m0 ++ m1)
          .map { kv ⇒ (kv._2.lastKey, kv._1.head + "-" + kv._1.last + "  " + kv._2.values.mkString(", ")) }
          .sortByKey()
          .map(_._2)
          .collectAsync()
          .map { rdd ⇒
            val r = (stageNames zip rdd).map(kv ⇒ s"${kv._1} ${kv._2}")
            PlayoffStandingView(r.size, r.toList, System.currentTimeMillis() - start)
          }
      }
    }

    implicit def season(implicit ex: ExecutionContext) = new StandingJournalTask[SeasonStandingView] {
      override val name = "[spark-task]: SeasonStandingView"
      override def async(ctx: SparkContext, config: Config, teams: mutable.HashMap[String, String], interval: Interval): Future[SeasonStandingView] = {
        val start = System.currentTimeMillis
        val rdd: RDD[(String, ByteBuffer)] = ctx.cassandraJournalRdd(config, teams.keySet).cache()

        rdd.mapValues(deserialize(_))
          .collect {
            case (team, event) if interval contains new DateTime(event.getResult.getTime).withZone(SCENTER_TIME_ZONE) ⇒
              NbaResultView(event.getResult.getHomeTeam, event.getResult.getHomeScore, event.getResult.getAwayTeam,
                event.getResult.getAwayScore, new Date(event.getResult.getTime))
          }.flatMap { r ⇒
            if (r.homeScore > r.awayScore) List((r.homeTeam, "hw"), (r.awayTeam, "al"))
            else List((r.homeTeam, "hl"), (r.awayTeam, "aw"))
          }.aggregateByKey(Standing())(
            (acc: Standing, marker: String) ⇒ marker match {
              case "hw" ⇒ acc.copy(hw = acc.hw + 1, w = acc.w + 1)
              case "hl" ⇒ acc.copy(hl = acc.hl + 1, l = acc.l + 1)
              case "aw" ⇒ acc.copy(aw = acc.aw + 1, w = acc.w + 1)
              case "al" ⇒ acc.copy(al = acc.al + 1, l = acc.l + 1)
            }, { (f, s) ⇒ f.copy(f.team, f.hw + s.hw, f.hl + s.hl, f.aw + s.aw, f.al + s.al, f.w + s.w, f.l + s.l) })
          .map(kv ⇒ kv._2.copy(team = kv._1))
          .sortBy(_.w, ascending = false)
          .collectAsync().map { seq ⇒
            val (west, east) = seq.partition { item ⇒
              teams(item.team) match {
                case "west" ⇒ true
                case "east" ⇒ false
              }
            }
            SeasonStandingView(west.size + east.size, west.toList, east.toList, System.currentTimeMillis() - start)
          }
      }
    }
  }


  object LeadersJournalTask {
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.SQLContext
    import scala.collection.JavaConverters._

    case class PlayerStats(gn: Int = 0, points: Double = 0.0, avg: Double = 0.0)

    @implicitNotFound(msg = "Cannot find LeadersJournalTask type class for ${T}")
    def apply[T <: SparkJobView: LeadersJournalTask](implicit ex: ExecutionContext) = implicitly[LeadersJournalTask[T]]

    implicit def ptsLeaders(implicit ex: ExecutionContext) = new LeadersJournalTask[PtsLeadersView] {
      override val name = "[spark-task]: journal-pts-leaders"
      override def async(ctx: SparkContext, config: Config, teams: mutable.HashMap[String, String], interval: Interval, depth: Int): Future[PtsLeadersView] = {
        val start = System.currentTimeMillis()
        val sqlContext = new SQLContext(ctx)
        import sqlContext.implicits._

        val rdd: RDD[(DateTime, ResultAddedEvent)] = ctx.cassandraJournalRdd2(config, teams.keySet).cache()
        val fd = rdd.collect {
          case (time, event) if (interval contains time) ⇒
            event.getResult.getHomeScoreBoxList.asScala.map(p ⇒ (p.getName, event.getResult.getHomeTeam, p.getPts))
              .++(event.getResult.getAwayScoreBoxList.asScala.map(p ⇒ (p.getName, event.getResult.getAwayTeam, p.getPts)))
        }.flatMap(identity).toDF("player", "team", "pts")

        fd
          .groupBy(fd("player"), fd("team"))
          .agg(fd("player"), fd("team"), avg(fd("pts")).as("points"), count(fd("pts")).as("games"))
          .sort($"points".desc)
          .limit(depth)
          .map { row ⇒ PtsLeader(row.getAs[String](3), row.getAs[String](0), row.getAs[Double](4).scale(2), row.getAs[Long](5)) }
          .collectAsync
          .map { leaders ⇒ PtsLeadersView(leaders.size, leaders.toList, System.currentTimeMillis() - start) }
      }
    }
  }

  object GamesJournalTask {
    import org.apache.spark.sql.SQLContext

    @implicitNotFound(msg = "Cannot find GamesJournalTask type class for ${T}")
    def apply[T <: SparkJobView: GamesJournalTask](implicit ex: ExecutionContext) = implicitly[GamesJournalTask[T]]

    implicit def view(implicit ex: ExecutionContext) = new GamesJournalTask[FilteredAllView] {
      override val name = "[spark-task]: StaticticsJob-FilteredView"
      override def async(ctx: SparkContext, config: Config, teams: mutable.HashMap[String, String], arenas: Seq[(String, String)],
                         interval: Interval, filteredTeams: Seq[String]): Future[FilteredAllView] = {

        val sqlContext = new SQLContext(ctx)
        import sqlContext.implicits._

        val start = System.currentTimeMillis()
        val rdd: RDD[(String, ByteBuffer)] = ctx.cassandraJournalRdd(config, teams.keySet).cache()

        val arenasDF = ctx.parallelize(arenas).toDF("HomeTeam", "Arena")
        val teamsDF = ctx.parallelize(filteredTeams).toDF("filteredId")

        val df = rdd.mapValues(deserialize(_))
          .collect {
            case event if (interval contains new DateTime(event._2.getResult.getTime).withZone(SCENTER_TIME_ZONE)) ⇒
              (event._2.getResult.getHomeTeam, event._2.getResult.getHomeScore, event._2.getResult.getAwayTeam,
                event._2.getResult.getAwayScore, extFormatter.format(new DateTime(event._2.getResult.getTime).withZone(SCENTER_TIME_ZONE).toDate))
          }.toDF("HomeTeam", "HomeScore", "AwayTeam", "AwayScore", "Date")

        ((teamsDF join (df, df("HomeTeam") === teamsDF("filteredId") || df("AwayTeam") === teamsDF("filteredId"))) join (arenasDF, "HomeTeam"))
          .sort(df("Date"))
          .map { row: Row ⇒
            ResultView(s"${row.getAs[String]("AwayTeam")} @ ${row.getAs[String]("HomeTeam")}",
              s"${row.getAs[Int]("AwayScore")} : ${row.getAs[Int]("HomeScore")}", row.getAs[String]("Date"), row.getAs[String]("Arena"))
          }.collectAsync.map { res ⇒ FilteredAllView(res.size, res.toList, System.currentTimeMillis() - start) }
      }
    }
  }
 */
}
