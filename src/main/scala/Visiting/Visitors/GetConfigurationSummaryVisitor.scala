package Visiting.Visitors

import Visiting.Components.ConfigVisitor
import Visiting.Configurations._

object GetConfigurationSummaryVisitor extends ConfigVisitor[String] {

  override def Visit(csvConfig: CsvSource): String = s"Delimiter=${csvConfig.delimiter}, HasHeader=${csvConfig.hasHeader}, Encoding=${csvConfig.encoding}, SkipLines=${csvConfig.skipLines}"

  override def Visit(jdbcConfig: SqlServerSource): String = s"Server=${jdbcConfig.server}, Database=${jdbcConfig.database}, Port=${jdbcConfig.port}"

  override def Visit(hiveTargetConfig: HiveTarget): String = s"Some hive target of death"

  override def Visit(parquetTargetConfig: ParquetTarget): String = s"ParquetDir=${parquetTargetConfig.parquetDir}, SaveMode=${parquetTargetConfig.saveMode}"

  override def Visit(fixedWidthConfig: FixedWidthSource): String = s"Columns=${fixedWidthConfig.columns.size}, "

  override def Visit(excelConfig: ExcelSource): String = s"DataAddress=${excelConfig.excelRange.GetDataAddress()}, HasHeader=${excelConfig.hasHeader}"
}
