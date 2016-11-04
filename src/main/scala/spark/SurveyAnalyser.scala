package spark

case class ServerLog(userId: Long,
                     sessionId: Long,
                     currentLocation: String,
                     searchLocation: String,
                     locationFilter: String,
                     minimumPriceFilter: Double,
                     maximumPriceFilter: Double) {
  def toCSV = s"$userId,$sessionId,$currentLocation,$searchLocation,$locationFilter,$minimumPriceFilter,$maximumPriceFilter"
}

case class Booking(userId: Long, bookedLocation: String, sessionId: Long)