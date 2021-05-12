package Visiting.Components

import org.apache.spark.sql.SparkSession

trait SourceConfig {
  private var target: TargetConfig = null
  def setTarget(target: TargetConfig) = this.target = target
  def targetAccept[TOut](visitor: TargetConfigVisitor[TOut]): TOut = target.accept((visitor))

  def accept[TOut](visitor: SourceConfigVisitor[TOut]): TOut
}