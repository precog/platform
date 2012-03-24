package com.precog.quirrel
package emitter

trait Solutions extends parser.AST {
  sealed trait Solution
  
  case class Conjunction(left: Solution, right: Solution) extends Solution
  case class Disjunction(left: Solution, right: Solution) extends Solution
  case class Definition(expr: Expr) extends Solution
}
