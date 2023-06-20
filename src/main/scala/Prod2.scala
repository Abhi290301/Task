import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types._

object Prod2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSVToKafkaJob")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    // Define the schema for the CSV data
    val schema = StructType(Seq(
      StructField("DateTime", StringType),
      StructField("LV_ActivePower", DoubleType),
      StructField("Wind_Speed", DoubleType),
      StructField("Theoretical_Power_Curve", DoubleType),
      StructField("Wind_Direction", DoubleType)
    ))

    // Read the CSV file using Spark Structured Streaming and the defined schema
    val csvStream = spark.readStream
      .format("csv")
      .option("header", true)
      .option("delimiter", "\t")
      .schema(schema)
      .load("C:\\tmp\\output\\Task\\TaskSchema\\")

    // Configure Kafka producer
    val kafkaBrokers = "localhost:9092"
    val kafkaTopic = "a"
    val kafkaProps = new java.util.Properties()
    kafkaProps.put("bootstrap.servers", kafkaBrokers)
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    import spark.implicits._

    // Convert each row of the CSV stream to a JSON string and publish to Kafka
    val kafkaStream = csvStream.select(
      functions.to_json(
        functions.struct($"DateTime", $"LV_ActivePower", $"Wind_Speed", $"Theoretical_Power_Curve", $"Wind_Direction")
      ).as("value")
    )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("topic", kafkaTopic)
      .option("checkpointLocation", "C:/tmp/CheckPoint")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    kafkaStream.awaitTermination()
    spark.stop()
  }
}
