package xede

import Visiting.Configurations._
import Visiting.Visitors._
import net.liftweb.json._
import net.liftweb.json.JsonDSL._
import net.liftweb.json.Serialization.{read, write}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Xede extends App {



  case class Simple(one: String, two: Boolean)

  override def main(args: Array[String]): Unit = {

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

//    val data = LoadDefinition(
//      source = CsvSource("|", true),
//      target = HiveTarget("TestDb", "TestTable", raw"C:\dev\xede-scala\testdata")
//    )
//
//    //val s = write(source)
//    val t = write(data)
//
//    println(t)
//
//    return

    val configUrl = args.head
    val dataSources = args.tail

    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .getOrCreate()



    val loadDefinitionJson = {
      val source = scala.io.Source.fromURL(configUrl)
      val json = source.mkString
      source.close()
      json
    }

    val loadDefinition = read[LoadDefinition](loadDefinitionJson)

    println(s"Writing:\n${dataSources.mkString("\n")}")
    println(s"Using Configuration\n$configUrl")
    println(s"From:\n${loadDefinition.source.accept(GetConfigurationSummaryVisitor)}")
    println(s"To:\n${loadDefinition.target.accept(GetConfigurationSummaryVisitor)}")

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
      val sourceDf = RenameColumns.rename(createDataFrameFunc(source))
      writeDataFrameFunc(sourceDf)
    })

  }
}
