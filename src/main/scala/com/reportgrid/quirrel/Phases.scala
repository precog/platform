package com.reportgrid.quirrel

trait Phases {
  type Expr
  type Error
  
  type Phase = Expr => Set[Error]
  
  private val Phases: List[Phase] =
    bindNames _ :: checkProvenance _ :: Nil
  
  def bindNames(tree: Expr): Set[Error]
  def checkProvenance(expr: Expr): Set[Error]
  
  def runPassesInSequence(tree: Expr): Set[Error] =
    Phases.foldLeft(Set[Error]()) { _ ++ _(tree) }
  
  def Error(node: Expr, tp: ErrorType): Error
  def showError(error: Error): String
}
