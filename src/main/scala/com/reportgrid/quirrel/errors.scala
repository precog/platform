package com.reportgrid.quirrel

trait StringErrors extends Passes {
  type Error = String
  
  def Error(node: Expr, msg: String): Error = msg
}
