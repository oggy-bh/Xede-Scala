package Visiting.Configurations

import Visiting.Components.{SourceConfig, SourceConfigVisitor}

case class SqlServerSource(tableOrQuery: String, server: String, database: String, port: Int, numPartitions: Int, fetchSize: Int) extends SourceConfig {
  override def accept[TOut](visitor: SourceConfigVisitor[TOut]): TOut = visitor.Visit(this)
}
