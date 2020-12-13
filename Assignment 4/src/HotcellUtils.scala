package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {

  val coordinateStep = 0.01
  val minX = -74.50 / coordinateStep
  val maxX = -73.70 / coordinateStep
  val minY = 40.50 / coordinateStep
  val maxY = 40.90 / coordinateStep
  val minZ = 1
  val maxZ = 31

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int = {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(", "").toDouble / coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")", "").toDouble / coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser(timestampString: String): Timestamp = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def checkneighbours(x: Long, y: Long, z: Long, neigh_x: Long, neigh_y: Long, neigh_z: Long): Boolean = {
    var returnVal = false
    if (neigh_x > maxX || neigh_x < minX || neigh_y < minY || neigh_y > maxY || neigh_z > maxZ || neigh_z < minZ) {
      returnVal = false
    }
    else if ((x - 1 <= neigh_x && neigh_x <= x + 1) && (y - 1 <= neigh_y && y + 1 >= neigh_y) && (z - 1 <= neigh_z && z + 1 >= neigh_z)) {
      returnVal = true
    }
    else {
      returnVal = false
    }
    returnVal
  }

  def getNeighbours(x: Long, y: Long, z: Long): Long = {
    var tempVal = 0
    for (i <- x - 1 to x + 1) {
      for (j <- y - 1 to y + 1) {
        for (k <- z - 1 to z + 1) {
          if ((i >= minX) && (i <= maxX) && (j >= minY) && (j <= maxY) & (k >= minZ & k <= maxZ)) {
            tempVal = tempVal + 1
          }
        }
      }
    }
    tempVal
  }


}