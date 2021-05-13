package xede

import Visiting.Configurations._
import Visiting.Visitors._
import net.liftweb.json.Serialization.read
import net.liftweb.json._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Xede extends App {

  override def main(args: Array[String]): Unit = {

    val configUrl = args.head
    val dataSources = args.tail

    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    implicit val formats: AnyRef with Formats = Serialization.formats(
      ShortTypeHints(
        List(
          classOf[CsvSource],
          classOf[ExcelSource],
          classOf[FixedWidthSource],
          classOf[SqlServerSource],
          classOf[HiveTarget]
        )
      )
    )

    val loadDefinitionJson = {
      val source = scala.io.Source.fromURL(configUrl)
      val json = source.mkString
      source.close()
      json
    }

    val loadDefinition = read[LoadDefinition](loadDefinitionJson)

    WriteSourceToTarget(
      loadDefinition,
      dataSources,
      spark
    )
  }

   def WriteSourceToTarget(loadDefinition: LoadDefinition, dataSources: Seq[String], spark: SparkSession): Unit = {

    val createDataFrameFunc: String => DataFrame = loadDefinition.source.accept(new CreateDataFrameVisitor(spark))

    val writeDataFrameFunc: DataFrame => Unit = loadDefinition.target.accept(new WriteDataFrameVisitor(spark))

    dataSources.par.foreach(source => {
      writeDataFrameFunc(createDataFrameFunc(source))
    })

  }
}
