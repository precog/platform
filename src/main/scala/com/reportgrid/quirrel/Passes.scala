package com.reportgrid.quirrel

trait Passes {
  type Expr
  type Error
  
  private type Pass = Expr => Set[Error]
  
  private val Passes: List[Pass] =
    bindNames _ :: checkProvenance _ :: Nil
  
  def bindNames(tree: Expr): Set[Error]
  def checkProvenance(expr: Expr): Set[Error]
  
  def runPassesInSequence(tree: Expr): Set[Error] = {
    Passes.foldLeft(Set[Error]()) { (errors, pass) =>
      if (!errors.isEmpty)
        errors
      else
        pass(tree)
    }
  }
  
  def Error(node: Expr, msg: String): Error
  def showError(error: Error): String
}
