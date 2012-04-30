package com.precog.quirrel

trait Phases {
  type Expr
  type Error

  type ConditionTree
  type GroupTree
  
  type Phase = Expr => Set[Error]
  
  private val Phases: List[Phase] =
    bindNames _ :: checkProvenance _ :: solveCriticalConditions _ :: inferBuckets _ :: Nil
  
  protected def LoadId: Identifier
  
  def bindNames(expr: Expr): Set[Error]
  def checkProvenance(expr: Expr): Set[Error]
  def solveCriticalConditions(expr: Expr): Set[Error]
  def inferBuckets(expr: Expr): Set[Error]
  
  def findCriticalConditions(expr: Expr): Map[String, Set[ConditionTree]]
  def findGroups(expr: Expr): Map[String, Set[GroupTree]]
  
  private[quirrel] def runPhasesInSequence(tree: Expr): Set[Error] =
    Phases.foldLeft(Set[Error]()) { _ ++ _(tree) }
  
  def Error(node: Expr, tp: ErrorType): Error
  def showError(error: Error): String
}
