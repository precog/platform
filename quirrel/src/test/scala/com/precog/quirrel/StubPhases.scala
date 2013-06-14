package com.precog.quirrel

import com.precog.util.BitSet

trait StubPhases extends Phases with RawErrors {
  protected def LoadId = Identifier(Vector(), "load")
  protected def RelLoadId = Identifier(Vector(), "relativeLoad")
  protected def ExpandGlobId = Identifier(Vector("std", "fs"), "expandPath")
  protected def DistinctId = Identifier(Vector(), "distinct")
  
  def bindNames(expr: Expr): Set[Error] = Set()
  def checkProvenance(expr: Expr): Set[Error] = Set()
  def inferBuckets(expr: Expr): Set[Error] = Set()
  def buildTrace(sigma: Sigma)(expr: Expr): Trace = Trace.empty
  def findCriticalConditions(expr: Expr): Map[String, Set[ConditionTree]] = Map()
}
