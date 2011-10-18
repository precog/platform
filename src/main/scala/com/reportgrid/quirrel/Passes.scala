package com.reportgrid.quirrel

trait Passes {
  type Expr
  type Error
  
  def bindNames(tree: Expr): Set[Error]
  
  def checkProvenance(expr: Expr): Set[Error]
  
  def Error(node: Expr, msg: String): Error
  
  def showError(error: Error): String
}
