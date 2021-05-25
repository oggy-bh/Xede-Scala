package Visiting.Visitors

import Visiting.Components.SourceConfigVisitor
import Visiting.Configurations._
import org.apache.commons.io.FilenameUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, DataFrameReader, Row, SparkSession}

import scala.collection.immutable

case class SourceData(dataframe: Seq[DataframeMetadata])

case class DataframeMetadata(dataFrame: DataFrame, tokens: Map[String, String])

object SourceData {
  def apply(dataFrame: DataFrame, filename: String): SourceData = SourceData(Seq(DataframeMetadata(dataFrame, Map("sourceName" -> filename))))
}

class CreateDataFrameVisitor(spark: SparkSession) extends SourceConfigVisitor[String => SourceData] {

  override def Visit(csvConfig: CsvSource): String => SourceData = {
    val optionedReader: DataFrameReader = spark.read
      .option("ignoreLeadingWhiteSpace", value = true) // you need this
      .option("ignoreTrailingWhiteSpace", value = true) // and this
      .option("delimiterValue", csvConfig.delimiter)
      .option("header", csvConfig.hasHeader)
      .option("inferSchema", value = false)
      .option("encoding", csvConfig.encoding)

    filename => {

      val outputFilename = FilenameUtils.getBaseName(filename)

      csvConfig.skipLines.fold(SourceData(optionedReader.csv(filename), outputFilename))(skipLines => {
        import spark.implicits._
        val rdd = spark.sparkContext.textFile(filename)
        val rddSkippedLines = rdd.mapPartitionsWithIndex((partitionIndex, r) => if (partitionIndex == 0) r.drop(skipLines) else r)
        val ds = spark.createDataset(rddSkippedLines)
        SourceData(optionedReader.csv(ds), outputFilename)
      })
    }
  }

  override def Visit(jdbcConfig: SqlServerSource): String => SourceData = {
    val jdbcUrl = makeJDBCUrl(jdbcConfig.database)

    val optionedReader = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("numPartitions", jdbcConfig.numPartitions)
      .option("fetchsize", jdbcConfig.fetchSize)

    tableOrQuery => {
      val (optionKey, filename) = isSelectRegEx.findFirstIn(tableOrQuery).fold(("dbtable", tableOrQuery))(_ => ("query", "raw_sql"))
      SourceData(optionedReader.option(optionKey, tableOrQuery).load(), filename)
    }
  }

  final private val isSelectRegEx = raw"(?i)(?=.*\bSELECT\b)(?=.*\bFROM\b).*".r

  final private val OLTP_JDBC_URL =
    "jdbc:sqlserver://brightoltp-prod.public.25032192a28f.database.windows.net:3342;user=sql-admin-user;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.25032192a28f.database.windows.net;loginTimeout=30;"

  final private def makeJDBCUrl(dbName: String): String = {
    val password = "" //dbutils.secrets.get(scope = "jdbc", key = "sql_password")
    val jdbcUrl = OLTP_JDBC_URL + "password=" + password + ";database=" + dbName + ";"
    jdbcUrl
  }

  override def Visit(fixedWidthConfig: FixedWidthSource): String => SourceData = { filename =>
    val rdd: RDD[String] = spark.sparkContext.textFile(filename)

    val fields = fixedWidthConfig.columns.map(fwc => StructField(fwc.name, StringType, nullable = true))

    val schema = StructType(fields)

    SourceData(
      spark.createDataFrame(rdd.map { line => splitStringIntoRow(fixedWidthConfig.columns, line) }, schema),
      FilenameUtils.getBaseName(filename)
    )
  }

  final private def splitStringIntoRow(list: Seq[FixedWidthColumn], str: String): Row = {

    //   val sb = str.toCharArray

    // list.map(_.width).foldLeft((0, Seq.empty[String]))((acc, cur) => {
    //   (acc._1 + cur, acc._2 :+ str.substring(acc._1, (cur - acc._1) + 1))
    // })

    val (_, result) = list.map(x => x.width).foldLeft((str, List[String]())) {
      case ((s, res), curr) =>
        if (s.length() <= curr) {
          val split = s.substring(0).trim()
          val rest = ""
          (rest, split :: res)
        } else if (s.length() > curr) {
          val split = s.substring(0, curr).trim()
          val rest = s.substring(curr)
          (rest, split :: res)
        } else {
          val split = ""
          val rest = ""
          (rest, split :: res)
        }
    }
    Row.fromSeq(result.reverse)
  }

  override def Visit(excelConfig: ExcelSource): String => SourceData = { filename =>
    SourceData(
      spark.read
      .format("com.crealytics.spark.excel")
      .option("dataAddress", excelConfig.excelRange.GetDataAddress()) // Optional, default: "A1"//    .option("dataAddress", "'My Sheet'!B3:C35") // Optional, default: "A1"
      .option("header", excelConfig.hasHeader) // Required
      .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
      .option("setErrorCellsToFallbackValues", "true") // Optional, default: false, where errors will be converted to null. If true, any ERROR cell values (e.g. #N/A) will be converted to the zero values of the column's data type.
      .option("usePlainNumberFormat", "false") // Optional, default: false, If true, format the cells without rounding and scientific notations
      .option("inferSchema", "false") // Optional, default: false
      //.option("addColorColumns", "false") // Optional, default: false
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
      .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
      .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from//    .option("workbookPassword", "pass") // Optional, default None. Requires unlimited strength JCE for older JVMs//    .schema(myCustomSchema) // Optional, default: Either inferred schema, or all columns are Strings
      .load(filename),
      FilenameUtils.getBaseName(filename)
    )
  }

  override def Visit(headerFooterConfig: HeaderFooterSource): String => SourceData = {
    fileOrDir => {
      val typeToColumnMap = headerFooterConfig.types
        .foldLeft(Map.empty[String, Seq[Column]])((tAcc, tCur) => {

          val columns = tCur.columns.foldLeft((Seq.empty[Column], 1))((cAcc, cCur) => {
            val column = trim(substring(col("value"), cAcc._2, cCur.width)).alias(cCur.name)
            cAcc.copy(_1 = cAcc._1 :+ column, cCur.width + cAcc._2)
          })

          tAcc.updated(tCur.delimiterValue, columns._1)
        })

      val commonDf = spark.read
        .textFile(fileOrDir)
        .withColumn("type", substring(col("value"), headerFooterConfig.delimiterIndex, headerFooterConfig.delimiterWidth))

      SourceData(
        typeToColumnMap.map(x =>
          DataframeMetadata(commonDf.filter(col("type") === lit(x._1)),
          Map("sourceName" -> fileOrDir, "type" -> x._1))
        ).toSeq
      )
    }
  }
}
