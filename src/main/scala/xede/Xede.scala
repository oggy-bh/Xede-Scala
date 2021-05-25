package xede

import Visiting.Configurations._
import Visiting.Utils.AppendLineage
import Visiting.Visitors._
import net.liftweb.json.Serialization.read
import net.liftweb.json._
import net.liftweb.json.ext.EnumSerializer
import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.collection.parallel.ForkJoinTaskSupport

object Xede extends App {


  override def main(args: Array[String]): Unit = {
    implicit val formats: Formats = Serialization.formats(
      ShortTypeHints(
        List(
          classOf[CsvSource],
          classOf[ExcelSource],
          classOf[FixedWidthSource],
          classOf[SqlServerSource],
          classOf[HiveTarget],
          classOf[ParquetTarget],
          classOf[HeaderFooterSource]
        )
      )
    ) + new EnumSerializer(MySaveMode)

    //    val data = LoadDefinition(
    //      source = HeaderFooterSource(6, 1, Seq(
    //        DelimiterType("H", Seq(
    //          // field 1, 1-5, CHAR Contract Number.
    //          FixedWidthColumn("contract_number", 5),
    //          // field 2, 6, CHAR Record Type Identifier. H = Header Record.
    //          FixedWidthColumn("record_identification_code", 1),
    //          // field 3, 7-56, CHAR Name of the Contract.
    //          FixedWidthColumn("contract_name", 50),
    //          // field 4, 57-62, CHAR Identified the month and year of payment. CCYYMM
    //          FixedWidthColumn("payment_cycle_date", 6),
    //          // field 5, 63-70, CHAR Identifies the date file was created. CCYYMMDD
    //          FixedWidthColumn("run_date", 8),
    //          // field 6, 71-250, CHAR Spaces.
    //          FixedWidthColumn("filler_6", 180)
    //        )),
    //        DelimiterType("C", Seq(
    //          // field 1, 1-5, CHAR Contract Number.
    //          FixedWidthColumn("contract_number", 5),
    //          // field 2, 6, CHAR Record Type Identifier. C = Capitated Payment
    //          FixedWidthColumn("record_identification_code", 1),
    //          // field 3, 7, CHAR 1.
    //          FixedWidthColumn("table_id_number", 1),
    //          // field 4, 8-9, CHAR Blank = for prospective pay.
    //          FixedWidthColumn("adjustment_reason_code", 2),
    //          // field 5, 10-18, NUM Number of beneficiaries for whom Part A payments is being made prospectively. For adjustment records this will hold the total number of transactions. ZZZZZZZZ9
    //          FixedWidthColumn("part_a_total_members", 9),
    //          // field 6, 19-27, NUM Number of beneficiaries for whom Part B payments is being made prospectively. Spaces for adjustment records. ZZZZZZZZ9
    //          FixedWidthColumn("part_b_total_members", 9),
    //          // field 7, 28-36, NUM Number of beneficiaries for whom Part D payments is being made prospectively. Spaces for Adjustment records. ZZZZZZZZ9
    //          FixedWidthColumn("part_d_total_members", 9),
    //          // field 8, 37-51, NUM Total Part A Amount. SSSSSSSSSSS9.99
    //          FixedWidthColumn("part_a_payment_amount", 15),
    //          // field 9, 52-66, NUM Total Part B Amount. SSSSSSSSSSS9.99
    //          FixedWidthColumn("part_b_payment_amount", 15),
    //          // field 10, 67-81, NUM Total Part D Amount. SSSSSSSSSSS9.99
    //          FixedWidthColumn("part_d_payment_amount", 15),
    //          // field 11, 82, – 96 NUM The Coverage Gap Discount Amount included in Part D Payment. SSSSSSSSSSS9.99
    //          FixedWidthColumn("coverage_gap_discount_amount", 15),
    //          // field 12, 97-, 111 NUM Total Payment. SSSSSSSSSSS9.99
    //          FixedWidthColumn("total_payment", 15),
    //          // field 13, 112, – 250 CHAR Spaces.
    //          FixedWidthColumn("filler_13", 139)
    //        ))
    //      )),
    //      target = ParquetTarget("C:\\dev\\testdata\\xede-output", MySaveMode.Overwrite),
    //      outputMask = "{baseName}_{type}",
    //      maxDegreeOfParallelism = Some(1)
    //    )
    //
    //
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
    val writeDataFrameFunc: DataframeMetadata => Unit = loadDefinition.target.accept(new WriteDataFrameVisitor(spark))

    dataSources.foreach(source => {

      val sourceData = createDataFrameFunc(source)

      val configTokens: Map[String, String] = loadDefinition.source.accept(AddTokensVisitor) ++
        loadDefinition.target.accept(AddTokensVisitor) ++
        CreateRuntimeTokens()
          .updated("outputMask", loadDefinition.outputMask)

      val parallelData = sourceData.dataframe.par

      parallelData.tasksupport = loadDefinition.maxDegreeOfParallelism.fold(parallelData.tasksupport)(maxDop =>
        new ForkJoinTaskSupport(new java.util.concurrent.ForkJoinPool(maxDop))
      )

      parallelData.foreach(
        dfm => {
          writeDataFrameFunc(
            dfm.copy(
              tokens = dfm.tokens ++ configTokens,
              dataFrame = AppendLineage.appendLineageToDf(dfm.tokens("sourceName"), RenameColumns.rename(dfm.dataFrame))
            )
          )
        })
    })

  }

  def CreateRuntimeTokens(): Map[String, String] = {

    val dateTime = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss")
    val date = DateTimeFormatter.ofPattern("yyyy_MM_dd")
    val time = DateTimeFormatter.ofPattern("HH_mm_ss")

    val justNow = LocalDateTime.now

    Map(
      "datetime" -> justNow.format(dateTime),
      "date" -> justNow.format(date),
      "time" -> justNow.format(time),
      "guid" -> UUID.randomUUID().toString,
      // examples we might want
      "environment" -> "dev",
      "cluster" -> "some-cluster",
      "tpid" -> "C0011234"
    )
  }
}
