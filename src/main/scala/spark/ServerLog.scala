package spark

case class ServerLog(userId: Long,
                     sessionId: Long,
                     currentLocation: String,
                     searchLocation: String,
                     locationFilter: String,
                     minimumPriceFilter: Double,
                     maximumPriceFilter: Double) {
  val minimumPriceFilterString = f"$minimumPriceFilter%1.2f"
  val maximumPriceFilterString = f"$maximumPriceFilter%1.2f"
  def toCSV = s"$userId,$sessionId,$currentLocation,$searchLocation,$locationFilter,$minimumPriceFilterString,$maximumPriceFilterString"
}


