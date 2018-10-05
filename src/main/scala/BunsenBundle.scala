package fi.codeo

import com.cerner.bunsen.{Bundles}
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.hl7.fhir.dstu3.model.Condition

object BunsenBundle {

  val bundles = Bundles.forStu3()

  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Bunsen Bundle")
      .master("local[*]")
      .getOrCreate()

    val fhirBundles = bundles.loadFromDirectory(
      spark,
      "./data/synthea-fhir-stu3/fhir",
      1
    )

    val conditionDs = bundles.extractEntry(
      spark,
      fhirBundles,
      classOf[Condition]
    )

    conditionDs.printSchema

    // Warnings in log:
    // WARN LenientErrorHandler: Unknown element 'fullUrl' found while parsing
    // WARN LenientErrorHandler: Unknown element 'resource' found while parsing
    conditionDs.show(5)
  }

}
  