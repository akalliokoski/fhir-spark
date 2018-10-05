package fi.codeo

import ca.uhn.fhir.context.FhirContext
import com.cerner.bunsen.FhirEncoders
import org.apache.spark.sql.{SparkSession}
import org.apache.log4j._
import org.apache.spark.api.java.function.MapFunction
import org.hl7.fhir.dstu3.model.{Condition}

abstract class BundleMapFunction extends MapFunction[String, Condition] {
  def call(bundleString: String): Condition
}

object BunsenTest {

  val ctx = FhirContext.forDstu3()

  val encoders = FhirEncoders.forStu3.getOrCreate

  val conditionMapFunction = new BundleMapFunction {
    override def call(bundleString: String): Condition = {
      return ctx.newJsonParser().parseResource(bundleString).asInstanceOf[Condition]
    }
  }

  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Bunsen Test")
      .master("local[*]")
      .getOrCreate()

    // NOTE: "multiline" option for reading multiline JSON causes
    // java.lang.IllegalAccessError: tried to access method com.google.common.base.Stopwatch.<init>()V from class org.apache.hadoop.mapreduce.lib.input.FileInputFormat

    import spark.implicits._

    val bundleJsonDf = spark
      .read
      //.option("multiLine", true)
      .text("./data/test-condition.json")
      .as[String]

    bundleJsonDf.show()

    val bundleDf = bundleJsonDf.map(
      conditionMapFunction,
      encoders.of(classOf[Condition])
    )

    bundleDf.show()
  }

}
  