package Visiting.Components

import Visiting.Configurations.{CsvSource, SqlServerSource, FixedWidthSource, ExcelSource}

trait SourceConfigVisitor[TOut] {
  def Visit(csvConfig: CsvSource): TOut

  def Visit(jdbcConfig: SqlServerSource): TOut

  def Visit(fixedWidthConfig: FixedWidthSource): TOut

  def Visit(excelConfig: ExcelSource): TOut
}
