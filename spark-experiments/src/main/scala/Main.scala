import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Main {
  def generateDf(sparkSession: SparkSession, numRecords: Long): DataFrame = {
    val generatorFunc = udf((id: Long) => f"user-name-${id}%09d")
    sparkSession.range(0L, numRecords).select(
      col("id"),
      generatorFunc(col("id")).as("user_name")
    )
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[4]")  // 4 threads
      .getOrCreate()

    val numRecords = args(0).toLong
    val df = generateDf(spark, numRecords)

    val url = "jdbc:mysql://127.0.0.1:3306/my_db?rewriteBatchedStatements=true"
    val user = "my"
    val pass = "my"
    val table = "my_table"

    df.write
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", url)
      .option("user", user)
      .option("password", pass)
      .option("dbtable", table)
      .option("truncate", "true")
      .option("numPartitions", "4")
      .option("batchsize", "10000")
      .mode(SaveMode.Overwrite)
      .save()
  }
}
