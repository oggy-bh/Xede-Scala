package Visiting.Visitors

import Visiting.Components.SourceConfigVisitor
import Visiting.Configurations.{CsvSource, ExcelSource, FixedWidthSource, HeaderFooterSource, SqlServerSource}

object GatherInputSourcesVisitor extends SourceConfigVisitor[Seq[String]] {

  override def Visit(csvConfig: CsvSource): Seq[String] = Seq.empty

  override def Visit(jdbcConfig: SqlServerSource): Seq[String] = Seq(jdbcConfig.tableOrQuery)

  override def Visit(fixedWidthConfig: FixedWidthSource): Seq[String] = Seq.empty

  override def Visit(excelConfig: ExcelSource): Seq[String] = Seq.empty

  override def Visit(headerFooterConfig: HeaderFooterSource): Seq[String] = Seq.empty
}
