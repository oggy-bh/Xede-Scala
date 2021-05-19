package Visiting.Configurations

import Visiting.Components.{TargetConfig, TargetConfigVisitor}
import org.apache.spark.sql.SaveMode

final case class ParquetTarget(parquetDir: String, saveMode: SaveMode = SaveMode.Overwrite) extends TargetConfig {
  override def accept[TOut](visitor: TargetConfigVisitor[TOut]): TOut = visitor.Visit(this)
}
