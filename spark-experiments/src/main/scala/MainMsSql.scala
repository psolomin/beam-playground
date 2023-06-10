import Main.generateDf
import org.apache.spark.sql.SparkSession

object MainMsSql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[4]")  // 4 threads
      .getOrCreate()

    val numRecords = 10
    val df = generateDf(spark, numRecords)

    df.write.format("com.microsoft.sqlserver.jdbc.spark")
      .mode("overwrite")
      .option("url", "jdbc:sqlserver://localhost:1433;databaseName=tempdb;")
      .option("dbtable", "dbo.table_1")
      .option("user", "sa")
      .option("password", "a123456789!")
      .save()

    spark.read.format("com.microsoft.sqlserver.jdbc.spark")
      .option("url", "jdbc:sqlserver://localhost:1433;databaseName=tempdb;")
      .option("dbtable", "dbo.table_1")
      .option("user", "sa")
      .option("password", "a123456789!")
      .load()
      .show()
  }
}
