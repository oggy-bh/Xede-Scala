package Visiting.Configurations

import Visiting.Components.{SourceConfig, TargetConfig}

case class LoadDefinition(source: SourceConfig, target: TargetConfig) {
  source.setTarget(target)
  target.setSource(source)
}
