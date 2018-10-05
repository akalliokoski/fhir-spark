package fi.codeo

import ca.uhn.fhir.context.FhirContext
import com.cerner.bunsen.FhirEncoders
import org.apache.spark._
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import org.apache.log4j._
import org.apache.spark.api.java.function.MapFunction
import org.hl7.fhir.dstu3.model.{Bundle, Condition}


object BunsenTest {
  

  /** Our main function where the action happens */
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
    // => need to minify the JSONs first

    import spark.implicits._

    val bundleJsonDf = spark
      .read
      //.option("multiLine", true)
      .text("./data/test-collection.json")
      .as[String]

    bundleJsonDf.show()

    val ctx = FhirContext.forDstu3()
    class BundleMapFunction extends MapFunction[String, Condition] {
      override def call(bundleString: String): Condition = {
        return ctx.newJsonParser().parseResource(bundleString).asInstanceOf[Condition]
      }
    }

    val encoders = FhirEncoders.forStu3.getOrCreate

    val bundleDf = bundleJsonDf.map(
      new BundleMapFunction,
      encoders.of(classOf[Condition])
    )

    // Throws error: "Task not serializable: java.io.NotSerializableException"
    // because BundleMapFunction is not serializable
    bundleDf.show()
  }
    
}
  