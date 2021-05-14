package Visiting.Configurations

import Visiting.Components.{SourceConfig, SourceConfigVisitor}

case class CsvSource(delimiter: String, hasHeader: Boolean, encoding: String = "UTF-8", skipLines: Option[Int] = None) extends SourceConfig {
  override def accept[TOut](visitor: SourceConfigVisitor[TOut]): TOut = visitor.Visit(this)
}