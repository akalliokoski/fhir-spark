package fi.codeo

import ca.uhn.fhir.context.FhirContext
import com.cerner.bunsen.FhirEncoders
import org.apache.spark.sql.{SparkSession}
import org.apache.log4j._
import org.apache.spark.api.java.function.MapFunction
import org.hl7.fhir.dstu3.model.{Condition}

abstract class ConditionMapFunction extends MapFunction[String, Condition] {
  def call(bundleString: String): Condition
}

object BunsenCondition {

  val ctx = FhirContext.forDstu3()

  val encoders = FhirEncoders.forStu3.getOrCreate

  val conditionMapFunction = new ConditionMapFunction {
    override def call(conditionString: String): Condition = {
      return ctx.newJsonParser().parseResource(conditionString).asInstanceOf[Condition]
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

    import spark.implicits._

    val conditionJsonDf = spark
      .read
      .option("multiLine", true)
      .text("./data/test-condition.json")
      .as[String]

    conditionJsonDf.show()

    val conditionDf = conditionJsonDf.map(
      conditionMapFunction,
      encoders.of(classOf[Condition])
    )

    conditionDf.printSchema()
    conditionDf.show()
  }

}
  