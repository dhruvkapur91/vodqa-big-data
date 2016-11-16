package spark.generators

import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FunSuite, ShouldMatchers}
import spark.ServerLog

class ServerLogsGenerator extends FunSuite with ShouldMatchers with GeneratorDrivenPropertyChecks {

  val userIdGen: Gen[Long] = Gen.oneOf(1L to 10000L)

  val sessionIdGen: Gen[Long] = Gen.oneOf(876L to 100000L)

  val currentLocationGen: Gen[String] = Gen.oneOf('A' to 'Z').map(_.toString)
  val searchLocationGen: Gen[String] = Gen.oneOf(Gen.oneOf('A' to 'Z').map(_.toString), Gen.const(""))
  val locationFilterGen: Gen[String] = Gen.oneOf(Gen.oneOf('A' to 'Z').map(_.toString), Gen.const(""))
  val minimumPriceGen: Gen[Double] = Gen.oneOf(Gen.choose(1000.0, 8000.0), Gen.const(-1.0))
  val maximumPriceGen: Gen[Double] = Gen.oneOf(Gen.choose(4500.0, 18000.0), Gen.const(-1.0))

  val surveyLogGen: Gen[ServerLog] = for {
    userId <- userIdGen
    sessionId <- sessionIdGen
    currentLocation <- currentLocationGen
    searchLocation <- searchLocationGen
    locationFilter <- locationFilterGen
    minimumPrice <- minimumPriceGen
    maximumPrice <- maximumPriceGen
  } yield
    ServerLog(
      userId,
      sessionId,
      currentLocation,
      searchLocation,
      locationFilter = if (Math.random() > 0.2) {
        searchLocation
      } else {
        locationFilter
      }
      ,
      minimumPrice, maximumPrice)

  test("Generate some server logs...") {
    forAll(Gen.listOfN(1000, surveyLogGen)) {
      (serverLogs: List[ServerLog]) => {
        serverLogs.map(_.toCSV).foreach(println)
      }
    }
  }


}
