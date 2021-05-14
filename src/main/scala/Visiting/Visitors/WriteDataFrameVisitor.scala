package Visiting.Visitors

import Visiting.Components.TargetConfigVisitor
import Visiting.Configurations.HiveTarget
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class WriteDataFrameVisitor(val spark: SparkSession) extends TargetConfigVisitor[DataFrame => Unit] {
  override def Visit(config: HiveTarget): DataFrame => Unit = { (df) =>
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${config.hiveDbName} ;")
    spark.sql(s"DROP TABLE IF EXISTS ${config.hiveDbName}.${config.tableName}")

    val parquetDir = config.hiveDir + "\\nppes_parquet"

    df.write
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
}
