package Visiting.Components

trait ConfigVisitor[TOut] extends SourceConfigVisitor[TOut] with TargetConfigVisitor[TOut]
