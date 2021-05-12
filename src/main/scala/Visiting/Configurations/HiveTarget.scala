package Visiting.Configurations

import Visiting.Components.{SourceConfig, TargetConfig, TargetConfigVisitor}

case class HiveTarget(hiveDbName: String, tableName: String, hiveDir: String, sql: Option[String] = None) extends TargetConfig {
  override def accept[TOut](visitor: TargetConfigVisitor[TOut]): TOut = visitor.Visit(this)
}
