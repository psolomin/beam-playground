import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{lit, when}
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

  it should "be nullable" in {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(List(Row(0L))),
      StructType(Array(StructField("id", LongType, nullable = false)))
    )
    val schema = df.withColumn("c2", when(lit(true), lit("a"))).schema
    assert(schema.fields(0).name, "id")
    assert(schema.fields(1).name, "c2")
    assert(!schema.fields(0).nullable)
    assert(schema.fields(1).nullable)

    val df2 = spark.sql("select case when true then 'abc' end as my_col")
    assert(df2.schema.fields(0).nullable)
  }
}
