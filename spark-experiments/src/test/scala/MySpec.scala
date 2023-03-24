import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec

class MySpec extends AnyFlatSpec with DataFrameSuiteBase {
  it should "generate df" in {
    val df = Main.generateDf(spark, 2L)
    val expected = spark.createDataFrame(
      spark.sparkContext.parallelize(List(
        Row(0L, "user-name-000000000"), Row(1L, "user-name-000000001"))
      ),
      StructType(Array(
        StructField("id", LongType, nullable = false),
        StructField("user_name", StringType, nullable = true)
      ))
    )
    assertDataFrameEquals(expected, df)
  }
}
