package Visiting.Components

import Visiting.Configurations.{HiveTarget, ParquetTarget}

trait TargetConfigVisitor[TOut] {
  def Visit(hiveConfig: HiveTarget): TOut

  def Visit(parquetConfig: ParquetTarget): TOut
}
