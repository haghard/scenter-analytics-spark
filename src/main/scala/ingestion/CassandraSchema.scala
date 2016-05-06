package ingestion

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.{CassandraConnectorConf, CassandraConnector}
import org.apache.spark.SparkConf

/*
  CREATE KEYSPACE sport_center WITH replication = {'class': 'NetworkTopologyStrategy', 'east': '2', 'west': '2'}  AND durable_writes = true;
*/
trait CassandraSchema {

  lazy val resultColumns =
    SomeColumns("period", "team", "score_line", "score", "opponent", "opponent_score_line", "opponent_score", "date", "seq_number")

  lazy val leadersColumns =
    SomeColumns("period", "name", "pos", "min", "fgma", "threepma", "ftma", "minusslashplus", "offreb", "defreb", "totalreb", "ast", "pf", "steel", "to0", "blockshoot", "ba", "pts",
    "team", "opponent", "time")

  lazy val playerColumns =
    SomeColumns("name", "period", "team", "time", "pos", "min", "fgma", "threepma", "ftma", "minusslashplus", "offreb", "defreb", "totalreb",
    "ast", "pf", "steel", "to0", "blockshoot", "ba", "pts", "opponent")

  lazy val dailyResColumns =
    SomeColumns("period", "opponents", "year", "month", "day", "score", "guest_score", "score_line", "guest_score_line")

  def installSchema(conf: SparkConf, keySpace: String, table1: String, table2: String, table3: String, table4: String,
                    teams: scala.collection.mutable.HashMap[String, String]) = {
    val con = new CassandraConnector(CassandraConnectorConf(conf))

    con.withSessionDo {
      _.execute(s"""CREATE TABLE IF NOT EXISTS ${keySpace}.teams (processor_id text, description text, PRIMARY KEY (processor_id))""")
    }.one()


    for (kv <- teams) {
      con.withSessionDo {
        _.execute(s"INSERT INTO ${keySpace}.teams (processor_id, description) VALUES (?, ?) IF NOT EXISTS", kv._1, kv._2)
      }.one()
    }

    con.withSessionDo {
      _.execute(s"""CREATE TABLE IF NOT EXISTS ${keySpace}.campaign (campaign_id text, description text, PRIMARY KEY (campaign_id))""")
    }.one()

    /*
                       +------------------+------------------------+
                       |26Apr2015:opponent|26Apr2015:opponent_score|
      +----------------+------------------+------------------------+
      |season-15-16:okc|     hou          |          93            |
      +----------------+------------------+------------------------+


    select * from results_by_period where period = 'season-12-13' and team = 'sas';

    */
    con.withSessionDo {
      _.execute(s"""CREATE TABLE IF NOT EXISTS $keySpace.${table1} (
                    | period varchar,
                    | team varchar,
                    | date timestamp,
                    | opponent text,
                    | opponent_score int,
                    | opponent_score_line text,
                    | score int,
                    | score_line text,
                    | seq_number bigint,
                    | PRIMARY KEY ((period, team), date)) WITH CLUSTERING ORDER BY (date DESC)""".stripMargin)
    }.one()


    /*
                   +--------------------+--------------------+
                   |26Apr2015:J.Wall:pos|26Apr2015:J.Wall:min|
      +------------+--------------------+--------------------+
      |season-15-16|         guard      |      35:23         |
      +------------+--------------------+--------------------+


    select name, team, pts from leaders_by_period where period = 'season-12-13' LIMIT 10;

    */
    con.withSessionDo {
      _.execute(s"""CREATE TABLE IF NOT EXISTS $keySpace.${table2} (
           | period text,
           | time timestamp,
           | name text,
           | ast int,
           | ba int,
           | blockshoot int,
           | defreb int,
           | fgma text,
           | ftma text,
           | min text,
           | minusslashplus text,
           | offreb int,
           | opponent text,
           | pf int,
           | pos text,
           | pts int,
           | steel int,
           | team text,
           | threepma text,
           | to0 int,
           | totalreb int,
           | PRIMARY KEY (period, time, name)) WITH CLUSTERING ORDER BY (time ASC, name ASC)
         """.stripMargin)
    }.one()

    /*

                              +-------------+------------------+
                              |26Apr2015:pts|26Apr2015:opponent|
     +------------------------+-------------+------------------+
     |L.James:season-15-16:cle|27           |chi               |
     +------------------------+-------------+------------------+

     select name, pts, team, opponent, time from player_by_period_team where name = 'S. Curry' and period = 'season-12-13' and team = 'gsw';

    */
    con.withSessionDo {
      _.execute(s"""CREATE TABLE IF NOT EXISTS $keySpace.${table3} (
           |        name text,
           |        period text,
           |        team text,
           |        time timestamp,
           |        ast int,
           |        ba int,
           |        blockshoot int,
           |        defreb int,
           |        fgma text,
           |        ftma text,
           |        min text,
           |        minusslashplus text,
           |        offreb int,
           |        opponent text,
           |        pf int,
           |        pos text,
           |        pts int,
           |        steel int,
           |        threepma text,
           |        to0 int,
           |        totalreb int,
           |        PRIMARY KEY ((name, period, team), time)) WITH CLUSTERING ORDER BY (time ASC)""".stripMargin)
    }.one()

    /*

                  +--------------------------+--------------------------------+--------------------------+
                  |2015:10:29:"okc-hou":score|2015:10:29:"okc-hou":guest_score|2015:10:29:"mia-cle":score|
     +---------------------------------------+--------------------------------+--------------------------+
     |season-15-16|89                        |89                              | 67                       |
     +---------------------------------------+-----------------------------------------------------------+

    select * from daily_results where period = 'season-12-13' and year=2012  and month=11 and day=29;
    */

    con.withSessionDo {
      _.execute(
        s"""CREATE TABLE IF NOT EXISTS $keySpace.${table4} (
           |        period text,
           |        opponents text,
           |        year int,
           |        month int,
           |        day int,
           |        score int,
           |        guest_score int,
           |        score_line text,
           |        guest_score_line text,
           |        PRIMARY KEY ((period), year, month, day, opponents)) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, opponents ASC)
         """.stripMargin)
    }.one()
  }
}
