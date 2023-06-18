import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions}

object Analysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DeltaTableAnalysis")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    val deltaTablePath = "C:\\tmp\\output\\Task\\DeltaData"

    val deltaDF = spark.read.format("delta").load(deltaTablePath)

    // Print the Delta Table
    println("Delta table prints:")
    deltaDF.show(truncate = false)

    // Printing the data points per day
    println("Showing no. of data points per day")
    val datapointPerDayDF = deltaDF
      .groupBy("signal_date")
      .agg(countDistinct("signal_ts").alias("datapoints"))
      .sort("signal_date")
    datapointPerDayDF.show(truncate = false)

    // Calculate the average value of signals per hour
    println("Printing the average value of signals per hour:")
    deltaDF.printSchema() // Check the schema for column names

    val averageDF = deltaDF
      .withColumn("hour", hour(col("signal_ts")))
      .groupBy("hour")
      .agg(
        avg(col("signals.`LV ActivePower (kW)`")).as("avg_LV_ActivePower"),
        avg(col("signals.`Wind Speed (m/s)`")).as("avg_Wind_Speed"),
        avg(col("signals.`Theoretical_Power_Curve (KWh)`")).as("avg_Theoretical_Power_Curve"),
        avg(col("signals.`Wind Direction (°)`")).as("avg_Wind_Direction")
      )
      .filter(col("hour") =!= 0)
      .sort("hour")
    averageDF.show(truncate = false)

    import spark.implicits._

    // Add generation_indicator column based on LV ActivePower values
    val resultDF = averageDF.withColumn("generation_indicator",
      when($"avg_LV_ActivePower" < 200, "Low")
        .when($"avg_LV_ActivePower" >= 200 && $"avg_LV_ActivePower" < 600, "Medium")
        .when($"avg_LV_ActivePower" >= 600 && $"avg_LV_ActivePower" < 1000, "High")
        .when($"avg_LV_ActivePower" >= 1000, "Exceptional")
    )

    resultDF.show(truncate = false)

    // Create a DataFrame with the provided JSON
    val json =
      """
        |[
        |    {
        |        "sig_name": "LV ActivePower (kW)",
        |        "sig_mapping_name": "avg_LV_ActivePower"
        |    },
        |    {
        |        "sig_name": "Wind Speed (m/s)",
        |        "sig_mapping_name": "avg_Wind_Speed"
        |    },
        |    {
        |        "sig_name": "Theoretical_Power_Curve (KWh)",
        |        "sig_mapping_name": "avg_Theoretical_Power_Curve"
        |    },
        |    {
        |        "sig_name": "Wind Direction (°)",
        |        "sig_mapping_name": "avg_Wind_Direction"
        |    }
        |]
        |""".stripMargin


    val jsonDF = spark.read.json(Seq(json).toDS)
    jsonDF.show(false)
    // Join the resultDF with the jsonDF based on sig_mapping_name

    import org.apache.spark.sql.functions.{col, lit, when}

    // ...

    val finalDF = resultDF.join(broadcast(jsonDF), resultDF("generation_indicator") === jsonDF("sig_mapping_name"), "left_outer")
      .select(
        resultDF("hour"),
        resultDF("avg_LV_ActivePower").as("avg_LV_ActivePower"),
        resultDF("avg_Wind_Speed").as("avg_Wind_Speed"),
        resultDF("avg_Theoretical_Power_Curve").as("avg_Theoretical_Power_Curve"),
        resultDF("avg_Wind_Direction").as("avg_Wind_Direction"),
        when(jsonDF("sig_mapping_name").isNotNull, jsonDF("sig_name")).otherwise(lit("Unknown")).as("generation_indicator")
      )

    finalDF.show(truncate = false)


    spark.stop()
  }
}
