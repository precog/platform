package com.reportgrid.quirrel

trait Passes {
  type Expr
  type Error
  
  private type Pass = Expr => Set[Error]
  
  private val Passes: List[Pass] =
    bindNames _ :: checkProvenance _ :: Nil
  
  def bindNames(tree: Expr): Set[Error]
  def checkProvenance(expr: Expr): Set[Error]
  
  def runPassesInSequence(tree: Expr): Set[Error] =
    Passes.foldLeft(Set[Error]()) { _ ++ _(tree) }
  
  def Error(node: Expr, tp: ErrorType): Error
  def showError(error: Error): String
}
