package ingestion

import java.net.InetSocketAddress

import akka.actor.Actor
import join.cassandra.CassandraSource
import akka.stream._
import akka.stream.scaladsl._
import com.typesafe.config.Config
import com.datastax.driver.core._
import ingestion.SparkFunctions.Period
import org.apache.spark.streaming.receiver.ActorHelper
import domain.formats.DomainEventFormats.ResultAddedEvent

object Journal {
  import scala.concurrent.duration._

  val RefreshEvery = JournalChangesIngestion.StreamingBatchInterval seconds

  /*
    sequence_nr >= ?
    I use = to support serializable consistency for all 3 tables providing at-least-once write.
    Cassandra provides idempotency on schema level to deal with duplicates
  */
  def queryByKey(journal: String) =
    s"""
       |SELECT message FROM $journal WHERE
       |  persistence_id = ? AND
       |  partition_nr = ? AND
       |  sequence_nr >= ?
   """.stripMargin

  /**
   *
   *
   */
  def maxSeqNumber =
    s"""SELECT seq_number FROM results_by_period WHERE
       | period = ? AND
       | team = ?
       | limit 1
    """.stripMargin
}

class Journal(config: Config, cassandraHosts: Array[InetSocketAddress],
              teams: scala.collection.mutable.HashMap[String, String],
              gameIntervals: java.util.LinkedHashMap[Period, String]) extends Actor with ActorHelper {
  import Journal._
  import FlowGraph.Implicits._
  import scala.collection.JavaConverters._

  val bufferSize = 64
  /**
   * Target number of entries per partition (= columns per row).
   * The value from akka-persistence-cassandra
   */
  val targetPartitionSize = 500000l

  val decider: Supervision.Decider = {
    case ex ⇒
      println("Journal fetch error: " + ex.getMessage)
      Supervision.restart
  }

  var progresses = teams.map(_._1 -> 0l).toMap
  val journal = config.getString("spark.cassandra.journal.table")

  implicit val c = context.system.dispatcher

  implicit val Mat = ActorMaterializer(ActorMaterializerSettings(context.system)
    .withSupervisionStrategy(decider)
    .withInputBuffer(bufferSize, bufferSize))(context.system)

  implicit val session = (cassandraClient(ConsistencyLevel.QUORUM) connect config.getString("spark.cassandra.journal.keyspace"))

  def cassandraClient(cl: ConsistencyLevel): CassandraSource#Client = {
    val qs = new QueryOptions().setConsistencyLevel(cl).setFetchSize(500)
    Cluster.builder()
      .addContactPointsWithPorts(asJavaCollectionConverter(cassandraHosts.toIterable).asJavaCollection)
      .withQueryOptions(qs)
      .build
  }

  override def preStart(): Unit = {
    progresses = loadProgress(session)
    println(progresses)
    akka.stream.scaladsl.RunnableGraph.fromGraph(journalN(progresses)).run()(Mat)
  }

  private def loadProgress(session: Session): Map[String, Long] = {
    progresses.map { kv ⇒
      (kv._1, gameIntervals.values().asScala./:(0l) { (acc, c) ⇒
        val seqNum = fetchMax(c, kv._1, session)
        if (seqNum > acc) seqNum else acc
      })
    }
  }

  private def fetchMax(period: String, team: String, session: Session): Long = {
    val row = session.execute(maxSeqNumber, period, team).one()
    if (row == null) 0l else row.getLong("seq_number")
  }

  private def flow(teams: Map[String, Long]) = Source.fromGraph(
    FlowGraph.create() { implicit b ⇒
      val merge = b.add(Merge[ResultAddedEvent](teams.size))
      teams.foreach { kv ⇒
        (eventlog.Log[CassandraSource] from (queryByKey(journal), kv._1, kv._2, targetPartitionSize))
          .source
          .map(row ⇒ cassandra.deserialize(row.getBytes("message"))) ~> merge
      }
      SourceShape(merge.out)
    }
  )

  private def journalN(teams: Map[String, Long]): Graph[ClosedShape, Unit] = {
    FlowGraph.create() { implicit b ⇒
      flow(teams) ~> Sink.actorRef[ResultAddedEvent](self, 'UpdateCompleted)
      ClosedShape
    }
  }

  override def receive = {
    case event: ResultAddedEvent if (event.getResult.getHomeTeam.length > 0) ⇒
      val counter = progresses(event.getResult.getHomeTeam) + 1
      progresses = progresses.updated(event.getResult.getHomeTeam, counter)
      store(event, counter)
    case 'UpdateCompleted ⇒
      context.system.scheduler.scheduleOnce(RefreshEvery)(
        akka.stream.scaladsl.RunnableGraph.fromGraph(journalN(progresses)).run()(Mat)
      )
  }
}