
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.{ Date, TimeZone }
import com.datastax.spark.connector._
import com.typesafe.config.Config
import http.NbaResult
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.{ DateTime, DateTimeZone }
import domain.formats.DomainEventFormats._
import scala.collection.JavaConverters._

package object cassandra {

  val offset = 8
  val SCENTER_TIME_ZONE = DateTimeZone.forID("EST") //New York

  def extFormatter = {
    val local = new SimpleDateFormat("dd MMM yyyy hh:mm a z")
    local.setTimeZone(TimeZone.getTimeZone("EST"))
    local
  }

  def formatter = {
    val local = new SimpleDateFormat("yyyy-MM-dd")
    local.setTimeZone(TimeZone.getTimeZone("EST"))
    local
  }

  private implicit class RichDate(date: DateTime) {
    def midnight: DateTime =
      date.withZone(SCENTER_TIME_ZONE).withTime(23, 59, 59, 0)
  }

  implicit class ExtendedDouble(n: Double) {
    def scale(x: Int) = {
      val w = Math.pow(10, x)
      (n * w).toLong.toDouble / w
    }
    def scale2(x: Int) = BigDecimal(n).setScale(2).toDouble
  }

  /*object SparkCassandraQuery {
    def apply[J <: SparkStandingJob[_]: ClassTag](implicit ex: ExecutionContext) = {
      implicitly[ClassTag[J]].runtimeClass.getConstructor(classOf[ExecutionContext])
        .newInstance(ex).asInstanceOf[J]
    }
  }*/

  val maxPartitionSize: Long = 5000000l

  def navigatePartition(sequenceNr: Long, maxPartitionSize: Long) = sequenceNr / maxPartitionSize

  def deserialize(message: ByteBuffer) = {
    def loop(bs: Array[Byte]): ResultAddedEvent =
      try {
        (ResultAddedEvent parseFrom bs)
      } catch {
        case e: Exception ⇒
          println(s"Parse proto error: ${e.getMessage}")
          ResultAddedEvent.getDefaultInstance
      }
    val bts = message.array()
    loop(bts.slice(offset, bts.length))
  }

  import com.github.nscala_time.time.TypeImports._
  def intervals(config: Config): scala.collection.mutable.LinkedHashMap[Interval, String] = {
    import scala.collection.JavaConverters._
    import com.github.nscala_time.time.Imports._
    var views = scala.collection.mutable.LinkedHashMap[Interval, String]()
    var start: Option[DateTime] = None
    var end: Option[DateTime] = None
    var period: Option[String] = None

    val stages = config.getConfig("app-settings").getObjectList("stages").asScala
      ./:(scala.collection.mutable.LinkedHashMap[String, String]()) { (acc, c) ⇒
        val it = c.entrySet().iterator()
        if (it.hasNext) {
          val entry = it.next()
          acc += (entry.getKey -> entry.getValue.render().replace("\"", ""))
        }
        acc
      }

    for ((k, v) ← stages) {
      if (start.isEmpty) {
        start = Some(new DateTime(v).midnight)
        period = Some(k)
      } else {
        end = Some(new DateTime(v).midnight)
        val interval = (start.get to end.get)
        views = views += (interval -> period.get)
        start = Some(end.get.withTime(23, 59, 59, 0))
        period = Some(k)
      }
    }
    views
  }

  implicit class SparkContextOps(context: SparkContext) {
    //limitation, currently bounded by 10 partitions == 5000000l * 10 records
    val partitions = new java.util.HashSet[Int]((0 to 10).asJavaCollection)

    def cassandraJournalRdd(config: Config, teams: scala.collection.Set[String]): RDD[(String, ByteBuffer)] = {
      val keyspace = config.getString("spark.cassandra.journal.keyspace")
      val table = config.getString("spark.cassandra.journal.table")
      val javaTeams = new java.util.HashSet[String](teams.asJavaCollection)
      (context broadcast javaTeams)
      (context broadcast partitions)
      context.cassandraTable[(String, ByteBuffer)](keyspace, table)
        .select("persistence_id", "message")
        .where("persistence_id in ? and partition_nr in ?", javaTeams, partitions)
    }

    def cassandraJournalRdd2(config: Config, teams: scala.collection.Set[String]): RDD[(DateTime, ResultAddedEvent)] = {
      val keyspace = config.getString("spark.cassandra.journal.keyspace")
      val table = config.getString("spark.cassandra.journal.table")
      val javaTeams = new java.util.HashSet[String](teams.asJavaCollection)
      (context broadcast javaTeams) //broadcast variable
      (context broadcast partitions) //broadcast variable
      context.cassandraTable(keyspace, table)
        .select("message")
        .where("persistence_id in ? and partition_nr in ?", javaTeams, partitions)
        .as((m: ByteBuffer) ⇒ deserialize(m))
        .keyBy(event ⇒ new DateTime(event.getResult.getTime).withZone(SCENTER_TIME_ZONE))
    }

    def cassandraJournalRdd(config: Config): RDD[CassandraRow] = {
      val keyspace = config.getString("spark.cassandra.journal.keyspace")
      val table = config.getString("spark.cassandra.journal.table")
      context.cassandraTable(keyspace, table).select("persistence_id", "message")
    }

    /**
     *
     * select * from results_by_period where period = 'season-12-13' and team in ('sas', 'cle', 'mia');
     */
    def cassandraStandingRdd(config: Config, teams: scala.collection.Set[String], period: String): RDD[NbaResult] = {
      val keyspace = config.getString("spark.cassandra.journal.keyspace")
      val table = config.getString("spark.cassandra.journal.results")
      val javaTeams = new java.util.HashSet[String](teams.asJavaCollection)
      (context broadcast javaTeams)
      context.cassandraTable[(String, Int, String, Int, Date)](keyspace, table)
        .select("team", "score", "opponent", "opponent_score", "date")
        .where("period = ? and team in ? ", period, javaTeams)
        .as((team: String, score: Int, opponent: String, opponent_score: Int, date: Date) ⇒ NbaResult(team, score, opponent, opponent_score, date))
    }

    /**
     *
     * select team, score, opponent, opponent_score, date from results_by_period where period = 'season-12-13' and team in ('sas', 'cle', 'mia');
     */
    def cassandraResultByPeriodRdd(config: Config, teams: scala.collection.Set[String],
                                   period: String): RDD[(String, Int, String, Int, java.sql.Date)] = {
      val keyspace = config.getString("spark.cassandra.journal.keyspace")
      val table = config.getString("spark.cassandra.journal.results")
      val javaTeams = new java.util.HashSet[String](teams.asJavaCollection)
      (context broadcast teams)
      context.cassandraTable[(String, Int, String, Int, Date)](keyspace, table)
        .select("team", "score", "opponent", "opponent_score", "date")
        .where("period = ? and team in ?", period, javaTeams)
        .as((team: String, score: Int, opponent: String, opponent_score: Int, date: java.sql.Date) ⇒ (team, score, opponent, opponent_score, date))
    }

    /**
     * select name, team, pts from leaders_by_period where period = 'season-15-16'
     */
    def cassandraPtsLeadersRdd(config: Config, period: String): RDD[((String, String), Float)] = {
      val keyspace = config.getString("spark.cassandra.journal.keyspace")
      val table = config.getString("spark.cassandra.journal.leaders")
      context.cassandraTable[(String, String, Float)](keyspace, table)
        .select("name", "team", "pts")
        .where("period = ?", period)
        .as((name: String, team: String, pts: Float) ⇒ ((name, team), pts))
    }

    /**
     * select name, team, offreb, defreb, totalreb from leaders_by_period where period = 'season-15-16'
     */
    def cassandraRebLeadersRdd(config: Config, period: String): RDD[((String, String), (Float, Float, Float))] = {
      val keyspace = config.getString("spark.cassandra.journal.keyspace")
      val table = config.getString("spark.cassandra.journal.leaders")
      context.cassandraTable[(String, String, Float)](keyspace, table)
        .select("name", "team", "offreb", "defreb", "totalreb")
        .where("period = ?", period)
        .as((name: String, team: String, offReb: Float, defReb: Float, totalReb: Float) ⇒ ((name, team), (offReb, defReb, totalReb)))
    }

    /**
     * select name, pts, team, opponent, time from player_by_name where name = 'S. Curry' and period = 'season-15-16' and team = 'gsw';
     */
    def cassandraPlayerRdd(config: Config, name: String, period: String, team: String): RDD[(String, Date, String, Int, Int, Int, String, String, String, Int, Int, Int, String, Int)] = {
      val keyspace = (config getString "spark.cassandra.journal.keyspace")
      val table = (config getString "spark.cassandra.journal.player")
      context.cassandraTable[(String, Date, String, Int, Int, Int, String, String, String, Int, Int, Int, String, Int)](keyspace, table)
        .select("opponent", "time", "min", "defreb", "offreb", "totalreb", "fgma", "threepma", "ftma", "ast", "blockshoot", "steel", "minusslashplus", "pts")
        .where("name = ? and period = ? and team = ?", name, period, team)
        .sortBy(_._2)
    }

    def cassandraPlayerRdd2(config: Config, name: String, period: String, team: String): RDD[(Date, Int, String, String, String, Int, Int, Int, Int)] = {
      val keyspace = (config getString "spark.cassandra.journal.keyspace")
      val table = (config getString "spark.cassandra.journal.player")
      context.cassandraTable[(Date, Int, String, String, String, Int, Int, Int, Int)](keyspace, table)
        .select("time", "totalreb", "fgma", "threepma", "ftma", "ast", "blockshoot", "steel", "pts")
        .where("name = ? and period = ? and team = ?", name, period, team)
        .sortBy(_._2)
    }

  }
}