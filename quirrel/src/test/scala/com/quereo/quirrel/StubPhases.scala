package com.quereo.quirrel

trait StubPhases extends Phases with RawErrors {
  def bindNames(tree: Expr): Set[Error] = Set()
  def checkProvenance(tree: Expr): Set[Error] = Set()
  def findCriticalConditions(expr: Expr): Map[String, Set[Expr]] = Map()
}
