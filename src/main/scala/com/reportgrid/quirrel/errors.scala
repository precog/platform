package com.reportgrid.quirrel

import edu.uwm.cs.gll.LineStream

trait StringErrors extends Passes {
  type Error = String
  
  def Error(node: Expr, msg: String): Error = msg
  
  def showError(error: Error) = error
}

trait LineErrors extends Passes with parser.AST {
  private val ErrorPattern = "error:%%d: %s%n    %%s%n    %%s"
  
  def showError(error: Error) = error.loc.formatError(ErrorPattern format error.msg)
  
  def Error(node: Expr, msg: String) = Error(node.loc, msg)
  
  case class Error(loc: LineStream, msg: String)
}
