package com.querio.quirrel

trait StubPhases extends Phases with RawErrors {
  def bindNames(expr: Expr): Set[Error] = Set()
  def checkProvenance(expr: Expr): Set[Error] = Set()
  def solveCriticalConditions(expr: Expr): Set[Error] = Set()
  def findCriticalConditions(expr: Expr): Map[String, Set[Expr]] = Map()
}
