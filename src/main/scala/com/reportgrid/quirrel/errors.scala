package com.reportgrid.quirrel

import edu.uwm.cs.gll.LineStream

trait RawErrors extends Phases {
  type Error = ErrorType
  
  override def Error(node: Expr, tp: ErrorType): Error = tp
  
  def showError(error: Error) = error.toString
}

trait LineErrors extends Phases with parser.AST {
  private val ErrorPattern = "error:%%d: %s%n    %%s%n    %%s"
  
  def showError(error: Error) = error.loc.formatError(ErrorPattern format error.tp)
  
  override def Error(node: Expr, tp: ErrorType) = Error(node.loc, tp)
  
  case class Error(loc: LineStream, tp: ErrorType)
}


sealed trait ErrorType

case class UndefinedTicVariable(name: String) extends ErrorType {
  override def toString = "undefined tic-variable: %s".format(name)
}

case class UndefinedFunction(name: String) extends ErrorType {
  override def toString = "undefined function: %s".format(name)
}

case object OperationOnUnrelatedSets extends ErrorType {
  override def toString = "cannot perform operation on unrelated sets"
}

case object AlreadyRelatedSets extends ErrorType {
  override def toString = "cannot relate sets that are already related"
}

case class IncorrectArity(expected: Int, got: Int) extends ErrorType {
  override def toString = "incorrect number of parameters: expected %d, got %d".format(expected, got)
}

case class UnspecifiedRequiredParams(missing: Seq[String]) extends ErrorType {
  override def toString = "unconstrained parameters on function invoked without specification: " + (missing mkString ", ")
}

case object SetFunctionAppliedToSet extends ErrorType {
  override def toString = "cannot apply a set function to another set"
}

case object FunctionArgsInapplicable extends ErrorType {
  override def toString = "cannot apply function to specified arguments"
}

// intended to be a warning
case class UnusedLetBinding(id: String) extends ErrorType {
  override def toString = "binding '%s' defined but not referenced in scope".format(id)
}

case class UnusedTicVariable(id: String) extends ErrorType {
  override def toString = "function parameter %s defined but not referenced or constrained".format(id)
}
