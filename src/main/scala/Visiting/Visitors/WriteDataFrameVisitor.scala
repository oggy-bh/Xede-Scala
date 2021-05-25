package Visiting.Visitors

import Visiting.Components.TargetConfigVisitor
import Visiting.Configurations.{HiveTarget, MySaveMode, ParquetTarget}
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

case class OutputNames(filename: String, tableName: String)

class WriteDataFrameVisitor(val spark: SparkSession) extends TargetConfigVisitor[DataframeMetadata => Unit] {
  override def Visit(config: HiveTarget): DataframeMetadata => Unit = { (df) =>
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${config.hiveDbName} ;")
    spark.sql(s"DROP TABLE IF EXISTS ${config.hiveDbName}.${config.tableName}")

    val outputNames: OutputNames = createFromFileSource(df)

    val parquetDir = config.hiveDir + "\\" + outputNames.filename

    df.dataFrame
      .write
      .mode(SaveMode.Overwrite)
      .parquet(parquetDir)

    Thread.sleep(5000)

    val matDf = spark.read.parquet(parquetDir)

    if (config.sql.isDefined) {
      val createExternalSql =
        s"""
              |CREATE TABLE IF NOT EXISTS ${config.hiveDbName}.${config.tableName}
              |USING PARQUET LOCATION '${parquetDir}'""".stripMargin
      println(s"${config.hiveDbName}.${config.tableName} sql: $createExternalSql")
      spark.sql(createExternalSql)

    } else {

      val sqlString = config.sql.fold(s"SELECT * FROM ${config.tableName}")(sql => sql)
      println(s"createHiveTableDf() build materialized external hive tables for DB: ${config.hiveDbName}.${config.tableName} from df at: ${config.hiveDir} with sql: $sqlString")

      matDf.createOrReplaceTempView(config.tableName)
      spark.sql(s"CREATE TABLE IF NOT EXISTS ${config.hiveDbName}.${config.tableName} USING PARQUET LOCATION '${parquetDir}' AS $sqlString")
    }
  }

  override def Visit(config: ParquetTarget): DataframeMetadata => Unit = { df =>
    val outputFilename = createFromFileSource(df).filename

    val parquetDir = s"${config.parquetDir}\\${outputFilename}"

    df.dataFrame
      .write
      .mode(MySaveMode.toJavaEquivalent(config.saveMode))
      .options(config.options)
      .parquet(parquetDir)
  }

  private def createFromFileSource(dfm: DataframeMetadata): OutputNames = {
      val outputFilename = ReplaceTokens(dfm.tokens("outputMask"), dfm.tokens.updated("baseName", FilenameUtils.getBaseName(dfm.tokens("sourceName"))))
      OutputNames(outputFilename, outputFilename)
  }

  private def ReplaceTokens(source: String, tokens: Map[String, String]): String = {
    tokens.foldLeft(source)((acc, cur) => {
      acc.replace("{" + cur._1 + "}", cur._2)
    })
  }
}
