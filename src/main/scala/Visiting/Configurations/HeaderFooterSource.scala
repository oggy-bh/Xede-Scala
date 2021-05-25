package Visiting.Configurations

import Visiting.Components.{SourceConfig, SourceConfigVisitor}

case class DelimiterType(delimiterValue: String, columns: Seq[FixedWidthColumn])

case class HeaderFooterSource(delimiterIndex: Int, delimiterWidth: Int, types: Seq[DelimiterType]) extends SourceConfig {
  override def accept[TOut](visitor: SourceConfigVisitor[TOut]): TOut = visitor.Visit(this)
}
