import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import java.util.concurrent.TimeUnit

object MainStreaming {
  def main(args: Array[String]): Unit = {
    val checkPointPath = args(0)
    val sinkPath = args(1)

    val spark = SparkSession.builder()
      .master("local[4]")  // 4 threads
      .getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:19092")
      .option("subscribe", "raw")
      .load()

    df.selectExpr("cast(key as string)", "cast(value as string)")
      .selectExpr("key", "from_json(value, 'day string, id bigint') as j")
      .selectExpr("key", "j.day", "j.id")
      .writeStream
      .partitionBy("day")
      .format("parquet")
      .outputMode(OutputMode.Append())
      .option("path", sinkPath)
      .option("checkpointLocation", checkPointPath)
      .trigger(Trigger.ProcessingTime(60, TimeUnit.SECONDS))
      .start
      .awaitTermination
  }
}
