package Visiting.Configurations

import Visiting.Components.{SourceConfig, SourceConfigVisitor, TargetConfig}

case class FixedWidthSource(columns: Seq[FixedWidthColumn]) extends SourceConfig {
  override def accept[TOut](visitor: SourceConfigVisitor[TOut]): TOut = visitor.Visit(this)
}

case class FixedWidthColumn(name: String, width: Int)