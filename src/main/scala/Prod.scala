import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Prod {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSVToKafkaJob")
      .master("local")
      .getOrCreate()
spark.sparkContext.setLogLevel("OFF")
    // Read the CSV file
    val df = spark.read
      .option("header", true)
      .option("delimiter","\t")
      .csv("C:\\tmp\\output\\Task\\TaskSchema\\T1.csv")

    // Configure Kafka producer
    val kafkaBrokers = "localhost:9092,localhost:9093,localhost:9094,localhost:9095"
    val kafkaTopic = "a"
    val kafkaProps = new java.util.Properties()
    kafkaProps.put("bootstrap.servers", kafkaBrokers)
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Publish records to Kafka
    df.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] =>
      val producer = new KafkaProducer[String, String](kafkaProps)
      partition.foreach { row =>
        val record = new ProducerRecord[String, String](kafkaTopic, row.mkString("\t"))
        producer.send(record)
      }
      producer.close()
    }

    spark.stop()
  }
}
