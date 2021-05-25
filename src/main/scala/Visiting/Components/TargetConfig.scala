package Visiting.Components
trait TargetConfig {
  private var source: Option[SourceConfig] = None
  def setSource(source: SourceConfig): Unit = this.source = Some(source)
  def sourceAccept[TOut](visitor: SourceConfigVisitor[TOut]): TOut = this.source.get.accept(visitor)

  def accept[TOut](visitor: TargetConfigVisitor[TOut]): TOut
}