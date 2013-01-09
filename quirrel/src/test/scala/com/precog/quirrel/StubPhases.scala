package com.precog.quirrel

import scalaz.Tree

trait StubPhases extends Phases with RawErrors {
  protected def LoadId = Identifier(Vector(), "load")
  protected def ExpandGlobId = Identifier(Vector("std", "fs"), "expandPath")
  protected def DistinctId = Identifier(Vector(), "distinct")
  
  def bindNames(expr: Expr): Set[Error] = Set()
  def checkProvenance(expr: Expr): Set[Error] = Set()
  def inferBuckets(expr: Expr): Set[Error] = Set()
  def buildTrace(sigma: Map[Formal, Expr])(expr: Expr): Tree[(Map[Formal, Expr], Expr)] =
      Tree.node((sigma, expr), Stream.empty)
  def findCriticalConditions(expr: Expr): Map[String, Set[ConditionTree]] = Map()
}
