package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val pointsInRect = queryRectangle.split(",").map(_.toDouble)
    val pointList = pointString.split(",").map(_.toDouble)
    val minX = math.min(pointsInRect(0), pointsInRect(2))
    val maxX = math.max(pointsInRect(0), pointsInRect(2))
    val minY = math.min(pointsInRect(1), pointsInRect(3))
    val maxY = math.max(pointsInRect(1), pointsInRect(3))
    if ((pointList(1) >= minY && pointList(1) <= maxY) && (pointList(0) >= minX && pointList(0) <= maxX)) {
      true
    } else {
      false
    }
  }
}