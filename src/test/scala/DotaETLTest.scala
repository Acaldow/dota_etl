import DotaETL.withKDA
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class DotaETLTest extends AnyFunSpec with SparkSessionTestWrapper with Matchers{

  import spark.implicits._

  it("appends KDAs to a DataFrame") {

    val account_id = 5
    val sourceData = Seq(
      Row(99999, 1, 5, 10),
      Row(99999, 13, 1, 4),
      Row(99999, 3, 0, 2)
    )

    val sourceSchema = List(
      StructField("match_id", IntegerType, true),
      StructField("kills", IntegerType, true),
      StructField("deaths", IntegerType, true),
      StructField("assists", IntegerType, true)
    )

    val sourceDF = spark.createDataFrame(spark.sparkContext.parallelize(sourceData), StructType(sourceSchema))

    val actualDF = sourceDF.transform(withKDA())
    val results = actualDF.select("KDA").collect.map(_.getDouble(0))
    results should contain theSameElementsAs Seq(2.2D, 17D, 5D)
  }
}
