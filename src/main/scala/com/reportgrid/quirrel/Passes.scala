package com.reportgrid.quirrel

trait Passes {
  type Expr
  type Error
  
  def bindNames(tree: Expr): Set[Error]
  
  def Error(node: Expr, msg: String): Error
}
