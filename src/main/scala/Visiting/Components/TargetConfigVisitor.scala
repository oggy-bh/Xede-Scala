package Visiting.Components

import Visiting.Configurations.HiveTarget

trait TargetConfigVisitor[TOut] {
  def Visit(csvConfig: HiveTarget): TOut
}
