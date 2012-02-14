package com.precog.quirrel

trait Phases {
  type Expr
  type Error
  
  type ConditionTree
  
  type Phase = Expr => Set[Error]
  
  private val Phases: List[Phase] =
    bindNames _ :: checkProvenance _ :: solveCriticalConditions _ :: Nil
  
  def bindNames(expr: Expr): Set[Error]
  def checkProvenance(expr: Expr): Set[Error]
  def solveCriticalConditions(expr: Expr): Set[Error]
  
  def findCriticalConditions(expr: Expr): Map[String, Set[ConditionTree]]
  
  private[quirrel] def runPhasesInSequence(tree: Expr): Set[Error] =
    Phases.foldLeft(Set[Error]()) { _ ++ _(tree) }
  
  def Error(node: Expr, tp: ErrorType): Error
  def showError(error: Error): String
}
