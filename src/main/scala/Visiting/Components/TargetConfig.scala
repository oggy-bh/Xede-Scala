package Visiting.Components

import org.apache.spark.sql.SparkSession
import scala.annotation.switch

trait TargetConfig {
  private var source: SourceConfig = null
  def setSource(source: SourceConfig) = this.source = source
  def sourceAccept[TOut](visitor: SourceConfigVisitor[TOut]): TOut = this.source.accept(visitor)

  def accept[TOut](visitor: TargetConfigVisitor[TOut]): TOut
}

trait Visitor[TOut] {
  def visit(testMe: TestMe): TOut
  def visit(testYou: TestYou): TOut
}

trait Test {
  def accept[TOut](visitor: Visitor[TOut]): TOut
}

case class TestMe(prop1: Int) extends Test {
  override def accept[TOut](visitor: Visitor[TOut]): TOut = visitor.visit(this)
}

case class TestYou(attrib1: String) extends Test {
  override def accept[TOut](visitor: Visitor[TOut]): TOut = visitor.visit(this)
}

// case class TestThem(attrib1: String) extends Test {
//   override def accept[TOut](visitor: Visitor[TOut]): TOut = visitor.visit(this)
// }

object GetSomething extends Visitor[Boolean] {
  override def visit(testMe: TestMe): Boolean = testMe.prop1 > 6
  override def visit(testYou: TestYou): Boolean = testYou.attrib1.contains("something")
}

trait ModernVisitor[TOut] {
  def visit(unknownConcreteType: Test): TOut =
    unknownConcreteType match {
      case t: TestMe   => visit(t)
      case t: TestYou  => visit(t)
      // case t: TestThem => visit(t)
    }

  def visit(t: TestMe): TOut
  def visit(t: TestYou): TOut
  // def visit(t: TestThem): TOut
}

class GetSomethingMatchVisitor extends ModernVisitor[Boolean] {
  // override def visit(t: TestThem): Boolean = t.attrib1.size > 100
  override def visit(t: TestMe): Boolean = t.prop1 > 6
  override def visit(t: TestYou): Boolean = t.attrib1.contains("something")
}

object GetSomethingMatch {
  def doThing(testMe: Test): Boolean = {
    testMe match {
      case t: TestMe  => t.prop1 > 6
      case t: TestYou => t.attrib1.contains("something")
    }
  }
}

class Example {
  def TestVisitor(unknownConcreteType: Test): Boolean = {
    unknownConcreteType.accept(GetSomething)
  }

  def testSwitch(unknownConcreteType: Test): Boolean = {
    GetSomethingMatch.doThing(unknownConcreteType)
  }

  // def testModern(unknownConcreteType: Test): Boolean = {
  //   GetSomethingMatchVisitor.visit(unknownConcreteType)
  // }
}
