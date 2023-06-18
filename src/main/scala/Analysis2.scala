//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._
//
//object DeltaLakeAnalysis {
//  def main(args: Array[String]): Unit = {
//    // Step 1: Read the data from delta lake
//    val spark = SparkSession.builder()
//      .appName("DeltaLakeAnalysis")
//      .master("local")
//      .getOrCreate()
//
//    val data = spark.read.format("delta").load("/path/to/delta_lake_table")
//
//    // Step 2: Calculate number of datapoints per day on distinct signal_ts
//    val datapointsPerDay = data
//      .select(date_trunc("day", col("signal_ts")).as("date"))
//      .groupBy("date")
//      .agg(countDistinct("signal_ts").as("datapoints"))
//      .orderBy("date")
//
//    datapointsPerDay.show(false)
//
//    // Step 3: Average value of all the signals per hour
//    val avgSignalsPerHour = data
//      .select(date_trunc("hour", col("signal_ts")).as("hour"), col("LV ActivePower (kW)"), col("Wind Speed (m/s)"), col("Theoretical_Power_Curve (KWh)"), col("Wind Direction (°)"))
//      .groupBy("hour")
//      .agg(
//        avg("LV ActivePower (kW)").as("avg_LV_ActivePower"),
//        avg("Wind Speed (m/s)").as("avg_Wind_Speed"),
//        avg("Theoretical_Power_Curve (KWh)").as("avg_Theoretical_Power_Curve"),
//        avg("Wind Direction (°)").as("avg_Wind_Direction")
//      )
//      .orderBy("hour")
//
//    avgSignalsPerHour.show(false)
//
//    // Step 4: Add generation_indicator column based on LV ActivePower values
//    val updatedData = data.withColumn("generation_indicator", when(col("LV ActivePower (kW)") < 200, "Low")
//      .when(col("LV ActivePower (kW)") >= 200 && col("LV ActivePower (kW)") < 600, "Medium")
//      .when(col("LV ActivePower (kW)") >= 600 && col("LV ActivePower (kW)") < 1000, "High")
//      .when(col("LV ActivePower (kW)") >= 1000, "Exceptional")
//      .otherwise(null))
//
//    // Step 5: Create a dataframe with the provided JSON
//    val mappingData = Seq(
//      ("LV ActivePower (kW)", "active_power_average"),
//      ("Wind Speed (m/s)", "wind_speed_average"),
//      ("Theoretical_Power_Curve (KWh)", "theo_power_curve_average"),
//      ("Theoretical_Power_Curve (KWh)", "theo_power_curve_average"),
//      ("Wind Direction (°)", "wind_direction_average")
//    )
//
//    val mappingSchema = StructType(Seq(
//      StructField("sig_name", StringType, nullable = false),
//      StructField("sig_mapping_name", StringType, nullable = false)
//    ))
//import spark.implicits._
//    val mappingDF = spark.createDataFrame(mappingData).toDF(mappingSchema)
//
//    // Step 6: Perform broadcast join to update signal names in updatedData
//    val joinedData = updatedData.join(broadcast(mappingDF), updatedData("sig_name") === mappingDF("sig_name"), "left")
//      .drop(mappingDF("sig_name"))
//
//    joinedData.show(false)
//  }
//}
