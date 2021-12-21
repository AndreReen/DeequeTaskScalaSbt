import com.amazon.deequ.VerificationResult.{checkResultsAsDataFrame, successMetricsAsDataFrame}
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.constraints.ConstrainableDataTypes
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import org.apache.spark.sql.SparkSession

object DeequeCheck extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("Spark")
    .getOrCreate()

  val dataset = spark.read
    .option("sep", "\t")
    .option("header", "true")
    .csv("C:\\deequ\\amazon_reviews_us_Camera_v1_00.tsv")

  dataset.show(10)


  val verificationResult: VerificationResult = {

    VerificationSuite()
      // data to run the verification on
      .onData(dataset)
      // define a data quality check
      .addCheck(
        Check(CheckLevel.Error, "Review Check")
          .isContainedIn(("verified_purchase"), Array("N", "Y"))
          .hasPattern("review_date", "\\d{1,2}\\/\\d{1,2}\\/\\d{4}".r)
          .isComplete("review_id")
          .isUnique("review_id")
          .hasDataType("total_votes", ConstrainableDataTypes.Integral)
      ).run()


  }

  checkResultsAsDataFrame(spark, verificationResult).show(truncate = false)

  successMetricsAsDataFrame(spark, verificationResult).show(truncate = false)

}

