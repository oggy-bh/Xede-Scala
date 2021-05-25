package Visiting.Configurations

import Visiting.Components.{SourceConfig, TargetConfig}

case class LoadDefinition(
                           source: SourceConfig,
                           target: TargetConfig,
                           maxDegreeOfParallelism: Option[Int] = None,
                           outputMask: String = "{baseName}"
                         ) {
  source.setTarget(target)
  target.setSource(source)
}
