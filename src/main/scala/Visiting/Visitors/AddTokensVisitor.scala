package Visiting.Visitors

import Visiting.Components.ConfigVisitor
import Visiting.Configurations.{CsvSource, ExcelSource, FixedWidthSource, HiveTarget, ParquetTarget, SqlServerSource}

object AddTokensVisitor extends ConfigVisitor[Map[String, String]] {
  override def Visit(hiveConfig: HiveTarget): Map[String, String] = Map(
    "targetType" -> "hive",
    "hiveDir" -> hiveConfig.hiveDir,
    "hiveDbName" -> hiveConfig.hiveDbName,
    "tableName" -> hiveConfig.tableName
  )

  override def Visit(parquetConfig: ParquetTarget): Map[String, String] = ???

  override def Visit(csvConfig: CsvSource): Map[String, String] = ???

  override def Visit(jdbcConfig: SqlServerSource): Map[String, String] = ???

  override def Visit(fixedWidthConfig: FixedWidthSource): Map[String, String] = ???

  override def Visit(excelConfig: ExcelSource): Map[String, String] = ???
}
