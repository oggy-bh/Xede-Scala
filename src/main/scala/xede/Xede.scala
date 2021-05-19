package xede

import Visiting.Configurations._
import Visiting.Visitors._
import net.liftweb.json._
import net.liftweb.json.JsonDSL._
import net.liftweb.json.Serialization.{read, write}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

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
          classOf[HiveTarget],
          classOf[ParquetTarget]
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
    val createDataFrameFunc: String => SourceData = loadDefinition.source.accept(new CreateDataFrameVisitor(spark))
    val writeDataFrameFunc: SourceData => Unit = loadDefinition.target.accept(new WriteDataFrameVisitor(spark))

    dataSources.foreach(source => {
      val sourceData: SourceData = createDataFrameFunc(source)

      val configTokens: Map[String, String] = loadDefinition.source.accept(AddTokensVisitor) ++
        loadDefinition.target.accept(AddTokensVisitor) ++
        CreateRuntimeTokens

      sour

//      writeDataFrameFunc(sourceData.copy(dataframe = sourceData.dataframe.map(RenameColumns.rename)))
    })

  }

  val dateTime = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss")
  val date = DateTimeFormatter.ofPattern("yyyy_MM_dd")
  val time = DateTimeFormatter.ofPattern("HH_mm_ss")

  def CreateRuntimeTokens(): Map[String, String] = Map(
    "datetime" -> LocalDateTime.now.format(dateTime),
    "date" -> LocalDateTime.now.format(date),
    "time" -> LocalDateTime.now.format(time),
    "guid" -> UUID.randomUUID().toString,
    // examples we might want
    "environment" -> "dev",
    "cluster" -> "some-cluster",
    "tpid" -> "C0011234"
  )
}
