package com.precog.quirrel

trait StubPhases extends Phases with RawErrors {
  protected def LoadId = Identifier(Vector(), "load")
  protected def DistinctId = Identifier(Vector(), "distinct")
  
  def bindNames(expr: Expr): Set[Error] = Set()
  def checkProvenance(expr: Expr): Set[Error] = Set()
  def inferBuckets(expr: Expr): Set[Error] = Set()
  def findCriticalConditions(expr: Expr): Map[String, Set[ConditionTree]] = Map()
  def findGroups(expr: Expr): Set[GroupTree] = Set()
}
