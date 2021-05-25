package Visiting.Configurations

import Visiting.Components.{TargetConfig, TargetConfigVisitor}
import Visiting.Configurations.MySaveMode.MySaveMode
import org.apache.spark.sql.SaveMode

object MySaveMode extends Enumeration {
  type MySaveMode = Value

  val Overwrite, ErrorIfExists, Ignore, Append = Value

  def toJavaEquivalent(v: MySaveMode): SaveMode = {
    v match {
      case Overwrite => SaveMode.Overwrite
      case ErrorIfExists => SaveMode.ErrorIfExists
      case Ignore => SaveMode.Ignore
      case Append => SaveMode.Append
    }
  }
}

final case class ParquetTarget(
                                parquetDir: String,
                                saveMode: MySaveMode = MySaveMode.Overwrite,
                                options: Map[String, String] = Map.empty
                              ) extends TargetConfig {
  override def accept[TOut](visitor: TargetConfigVisitor[TOut]): TOut = visitor.Visit(this)
}
