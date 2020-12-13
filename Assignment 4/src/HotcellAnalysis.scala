package cse512

import java.util
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._


object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)



  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    //  pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ", (pickupTime: String) => ((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
    //    pickupInfo.show()


    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)


    // changed code from here

    val x_range = maxX.toInt - minX.toInt + 1

    val y_range = maxY.toInt - minY.toInt + 1
    val z_range = maxZ.toInt - minZ.toInt + 1

    val countInf = pickupInfo.groupBy("x", "y", "z").count().persist()
    countInf.createOrReplaceTempView("countride")


    var countPickup = pickupInfo.count() / numCells
    var total = spark.sql("SELECT sum(count*count) FROM countride").first().get(0).toString.toDouble
    var sd = math.sqrt(total / numCells - (countPickup * countPickup))
    spark.udf.register("checkneighbours", (x: Double, y: Double, z: Double, neigh_x: Double, neigh_y: Double, neigh_z: Double) => ((
      HotcellUtils.checkneighbours(x.toLong, y.toLong, z.toLong, neigh_x.toLong, neigh_y.toLong, neigh_z.toLong)
      )))

    spark.udf.register("getNeighbours", (x: Long, y: Long, z: Long) => ((
      HotcellUtils.getNeighbours(x.toLong, y.toLong, z.toLong)
      )))

    def getGscore(neighCountReduced: Double, neighCount: Double): Double = {
      var fractionTop = neighCountReduced - countPickup * neighCount
      var fractionBottom = sd * math.sqrt(((numCells * neighCount) - (neighCount * neighCount)) / (numCells - 1))
      var gScore = fractionTop / fractionBottom
      if (gScore.toString.equals("NaN")) println(fractionTop + " " + fractionBottom + " " + math.sqrt(((numCells * neighCount) - (neighCount * neighCount)) / (numCells - 1)))
      return gScore
    }

    spark.udf.register("getGscore", (neighCountReduced: Double, neighCount: Double) => ((
      getGscore(neighCountReduced, neighCount)
      )))

    var totNeighbors = spark.sql("SELECT trans1.x, trans1.y, trans1.z, sum(trans2.count) AS neighCountReduced, " +
      "getNeighbours(trans1.x, trans1.y, trans1.z) AS neighCount " +
      "FROM countride trans1 CROSS JOIN countride trans2 where " +
      "checkneighbours(trans1.x, trans1.y, trans1.z, trans2.x, trans2.y, trans2.z) group by trans1.x,trans1.y,trans1.z").persist()
    totNeighbors.createOrReplaceTempView("totNeighbors")

    var finalResult = spark.sql("SELECT x, y, z, getGscore(neighCountReduced, neighCount) AS zs FROM totNeighbors ORDER BY zs DESC ")
    finalResult.createOrReplaceTempView("finalResult")
    var outputResult = spark.sql("SELECT x, y, z FROM finalResult")
    return outputResult
  }

}
