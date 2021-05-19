package Visiting.Configurations

import Visiting.Components.{TargetConfig, TargetConfigVisitor}

case class ParquetTarget(parquetDir: String, parquetFilename: String) extends TargetConfig {
  override def accept[TOut](visitor: TargetConfigVisitor[TOut]): TOut = visitor.Visit(this)
}
