package Visiting.Components

trait SourceConfig {
  private var target: Option[TargetConfig] = None
  def setTarget(target: TargetConfig) = this.target = Some(target)
  def targetAccept[TOut](visitor: TargetConfigVisitor[TOut]): TOut = target.get.accept((visitor))

  def accept[TOut](visitor: SourceConfigVisitor[TOut]): TOut
}