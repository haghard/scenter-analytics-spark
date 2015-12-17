import java.util.Date

import cassandra.{NbaResult, PlayerLine, ResultAdded, Total}
import domain.formats.DomainEventFormats.{ResultAddedEvent, ResultFormat}
import org.scalatest.{MustMatchers, WordSpec}

class ProtobufSpec extends WordSpec with MustMatchers {

  //Sat Nov 03 05:00:00 MSK 2012
  "parse config with a list of contact points without port" in {
    val DO = ResultAdded("okc",
      NbaResult("okc", 106, "por", 92, new Date(), "26-23-25-32", "21-21-24-26",
        Total(240, "39-76", "5-9", "23-32", "", 8, 39, 47, 18, 22, 2, 12, 10, 0, 106),
        Total(240, "32-89", "9-26", "19-24", "", 11, 33, 44, 17, 25, 4, 11, 0, 10, 92),
        List(PlayerLine("K. Durant", "F", "42:41", "7-14", "1-3", "8-12", "+13", 2, 15, 17, 7, 1, 0, 6, 2, 0, 23),
          PlayerLine("S. Ibaka", "F-C", "26:40", "3-11", "0-0", "1-2", "+3", 1, 4, 5, 0, 4, 0, 1, 2, 0, 7),
          PlayerLine("K. Perkins", "C", "21:12", "0-2", "0-0", "0-0", "-4", 1, 2, 3, 0, 0, 0, 1, 1, 0, 0),
          PlayerLine("T. Sefolosha", "G", "18:16", "1-3", "0-0", "0-0", "0", 1, 2, 3, 0, 2, 0, 0, 1, 0, 2),
          PlayerLine("R. Westbrook", "G", "35:17", "13-24", "0-2", "6-8", "+7", 1, 4, 5, 6, 1, 0, 1, 0, 0, 32),
          PlayerLine("N. Collison", "", "27:38", "5-6", "0-0", "2-2", "+11", 0, 5, 5, 2, 5, 1, 1, 1, 0, 12),
          PlayerLine("K. Martin", "", "28:48", "5-11", "3-3", "6-6", "+11", 1, 2, 3, 2, 2, 1, 0, 1, 0, 19),
          PlayerLine("H. Thabeet", "", "17:31", "2-2", "0-0", "0-1", "+13", 1, 3, 4, 0, 6, 0, 1, 2, 0, 4),
          PlayerLine("E. Maynor", "", "15:30", "1-1", "0-0", "0-1", "+12", 0, 0, 0, 0, 1, 0, 1, 0, 0, 2),
          PlayerLine("P. Jones", "", "05:19", "1-1", "0-0", "0-0", "+1", 0, 2, 2, 1, 0, 0, 0, 0, 0, 2),
          PlayerLine("J. Lamb", "", "01:08", "1-1", "1-1", "0-0", "+3", 0, 0, 0, 0, 0, 0, 0, 0, 0, 3),
          PlayerLine("R. Jackson", "DNP - Coach's Decision", "", "", "", "", "", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
          PlayerLine("D. Liggins", "DNP - Coach's Decision", "", "", "", "", "", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)),
        List(PlayerLine("K. Durant", "F", "42:41", "7-14", "1-3", "8-12", "+13", 2, 15, 17, 7, 1, 0, 6, 2, 0, 23),
          PlayerLine("S. Ibaka", "F-C", "26:40", "3-11", "0-0", "1-2", "+3", 1, 4, 5, 0, 4, 0, 1, 2, 0, 7),
          PlayerLine("K. Perkins", "C", "21:12", "0-2", "0-0", "0-0", "-4", 1, 2, 3, 0, 0, 0, 1, 1, 0, 0),
          PlayerLine("T. Sefolosha", "G", "18:16", "1-3", "0-0", "0-0", "0", 1, 2, 3, 0, 2, 0, 0, 1, 0, 2),
          PlayerLine("R. Westbrook", "G", "35:17", "13-24", "0-2", "6-8", "+7", 1, 4, 5, 6, 1, 0, 1, 0, 0, 32),
          PlayerLine("N. Collison", "", "27:38", "5-6", "0-0", "2-2", "+11", 0, 5, 5, 2, 5, 1, 1, 1, 0, 12),
          PlayerLine("K. Martin", "", "28:48", "5-11", "3-3", "6-6", "+11", 1, 2, 3, 2, 2, 1, 0, 1, 0, 19),
          PlayerLine("H. Thabeet", "", "17:31", "2-2", "0-0", "0-1", "+13", 1, 3, 4, 0, 6, 0, 1, 2, 0, 4),
          PlayerLine("E. Maynor", "", "15:30", "1-1", "0-0", "0-1", "+12", 0, 0, 0, 0, 1, 0, 1, 0, 0, 2),
          PlayerLine("P. Jones", "", "05:19", "1-1", "0-0", "0-0", "+1", 0, 2, 2, 1, 0, 0, 0, 0, 0, 2),
          PlayerLine("J. Lamb", "", "01:08", "1-1", "1-1", "0-0", "+3", 0, 0, 0, 0, 0, 0, 0, 0, 0, 3),
          PlayerLine("R. Jackson", "DNP - Coach's Decision", "", "", "", "", "", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
          PlayerLine("D. Liggins", "DNP - Coach's Decision", "", "", "", "", "", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)))
    )

    val array = eventBuilder(DO).build().toByteArray
    println(array.length)

    val restored = ResultAddedEvent.parseFrom(array)
    println(restored)
    restored === DO
  }

  private def eventBuilder(e: ResultAdded): ResultAddedEvent.Builder = {
    val res = e.r
    val format = ResultFormat.newBuilder()
      .setHomeTeam(res.homeTeam).setHomeScore(res.homeScore)
      .setAwayTeam(res.awayTeam).setAwayScore(res.awayScore)
      .setTime(res.dt.getTime)
      .setHomeScoreLine(res.homeScoreBox)
      .setAwayScoreLine(res.awayScoreBox)

    format.setHomeTotal(domain.formats.DomainEventFormats.TotalFormat.newBuilder()
      .setMinutes(res.homeTotal.min)
      .setFgmA(res.homeTotal.fgmA)
      .setThreePmA(res.homeTotal.threePmA)
      .setFtmA(res.homeTotal.ftmA)
      .setMinusSlashPlus(res.homeTotal.minusSlashPlus)
      .setOffReb(res.homeTotal.offReb)
      .setDefReb(res.homeTotal.defReb)
      .setTotalReb(res.homeTotal.totalReb)
      .setAst(res.homeTotal.ast)
      .setPf(res.homeTotal.pf)
      .setSteels(res.homeTotal.steels)
      .setTo(res.homeTotal.to)
      .setBs(res.homeTotal.bs)
      .setBa(res.homeTotal.ba)
      .setPts(res.homeTotal.pts)
      .build())

    format.setAwayTotal(domain.formats.DomainEventFormats.TotalFormat.newBuilder()
      .setMinutes(res.awayTotal.min)
      .setFgmA(res.awayTotal.fgmA)
      .setThreePmA(res.awayTotal.threePmA)
      .setFtmA(res.awayTotal.ftmA)
      .setMinusSlashPlus(res.awayTotal.minusSlashPlus)
      .setOffReb(res.awayTotal.offReb)
      .setDefReb(res.awayTotal.defReb)
      .setTotalReb(res.awayTotal.totalReb)
      .setAst(res.awayTotal.ast)
      .setPf(res.awayTotal.pf)
      .setSteels(res.awayTotal.steels)
      .setTo(res.awayTotal.to)
      .setBs(res.awayTotal.bs)
      .setBa(res.awayTotal.ba)
      .setPts(res.awayTotal.pts)
      .build())

    res.homeBox.foreach { r =>
      format.addHomeScoreBox(
        domain.formats.DomainEventFormats.PlayerLine.newBuilder()
          .setName(r.name).setPos(r.pos).setMin(r.min)
          .setFgmA(r.fgmA).setThreePmA(r.threePmA).setFtmA(r.ftmA)
          .setMinusSlashPlus(r.minusSlashPlus).setOffReb(r.offReb)
          .setDefReb(r.defReb).setTotalReb(r.totalReb)
          .setAst(r.ast).setPf(r.pf).setSteels(r.steels)
          .setTo(r.to).setBs(r.bs).setBa(r.ba).setPts(r.pts)
          .build())
    }

    res.awayBox.foreach { r =>
      format.addAwayScoreBox(
        domain.formats.DomainEventFormats.PlayerLine.newBuilder()
          .setName(r.name).setPos(r.pos).setMin(r.min)
          .setFgmA(r.fgmA).setThreePmA(r.threePmA).setFtmA(r.ftmA)
          .setMinusSlashPlus(r.minusSlashPlus).setOffReb(r.offReb)
          .setDefReb(r.defReb).setTotalReb(r.totalReb)
          .setAst(r.ast).setPf(r.pf).setSteels(r.steels)
          .setTo(r.to).setBs(r.bs).setBa(r.ba).setPts(r.pts)
          .build())
    }

    ResultAddedEvent.newBuilder()
      .setTeamName(e.team)
      .setResult(format.build())
  }
}