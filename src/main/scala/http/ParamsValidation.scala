package http

trait ParamsValidation {

  import cats.data.Validated
  import cats.implicits._
  import cats.data.Xor
  import cats.data.Validated
  import cats.implicits._

  def teams: scala.collection.mutable.HashMap[String, String]

  def intervals: scala.collection.mutable.LinkedHashMap[org.joda.time.Interval, String]

  def validateTeam(team: String): Validated[String, Unit] = {
    teams.get(team).fold(Validated.invalid[String, Unit](s"\n Could'n find team $team"))(r => Validated.valid[String, Unit](()))
  }

  def validateTeams(searchTeams: String): Validated[String, List[String]] = {
    val teamsFilter = searchTeams.split(",").toList
    implicitly[cats.Traverse[List]].traverseU(teamsFilter) { team =>
      teams.get(team).fold(Validated.invalid[String, String](s"\n Could'n find team $team"))(r => Validated.valid[String, String](r))
    }
  }

  def validatePeriod(season: String): Validated[String, Unit] = {
    (for { (k, v) ← intervals if (v == season) } yield k).headOption
      .fold(Validated.invalid[String, Unit](s"\n Could'n find season $season"))(_ => Validated.valid[String, Unit](()))
  }
}
