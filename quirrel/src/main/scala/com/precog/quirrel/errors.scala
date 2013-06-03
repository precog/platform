package com.precog.quirrel

import com.codecommit.gll.LineStream

trait Errors extends Phases {
  def isWarning(error: Error): Boolean
}

trait RawErrors extends Errors with Phases {
  type Error = ErrorType
  
  override val Error: ErrorCompanion = new ErrorCompanion {
    def apply(expr: Expr, tp: ErrorType) = tp
    def unapply(tp: ErrorType) = Some(tp)
  }
  
  def showError(error: Error) = error.toString
  
  override def isWarning(error: Error) = error match {
    case UnusedLetBinding(_) => true
    case DeprecatedFunction(_, _) => true
    case _ => false
  }
}

trait LineErrors extends Errors with Phases with parser.AST {
  private val ErrorPattern = "error:%%d: %s%n    %%s%n    %%s"
  
  def showError(error: Error) = error.loc.formatError(ErrorPattern format error.tp)
  
  override val Error: ErrorCompanion = new ErrorCompanion {
    def apply(expr: Expr, tp: ErrorType): Error = new Error(expr.loc, tp)
    def unapply(error: Error): Option[ErrorType] = Some(error.tp)
  }
  
  override def isWarning(error: Error) = error match {
    case Error(UnusedLetBinding(_)) => true
    case Error(UnableToSolveCriticalCondition(_)) => true
    case Error(DeprecatedFunction(_, _)) => true
    case _ => false
  }
  
  class Error(val loc: LineStream, val tp: ErrorType) {
    override def toString = "Error(<%d:%d>, %s)".format(loc.lineNum, loc.colNum, tp)
  }
}


sealed trait ErrorType 

case object CannotUseDistributionWithoutSampling extends ErrorType {
  override def toString = "cannot use distribution without sampling"
}

case class UndefinedTicVariable(name: TicId) extends ErrorType {
  override def toString = "undefined tic-variable: %s".format(name)
}

case class MultiplyDefinedTicVariable(name: TicId) extends ErrorType {
  override def toString = "tic-variable name used multiple times: %s".format(name)
}

case class UndefinedFunction(name: Identifier) extends ErrorType {
  override def toString = "undefined name: %s".format(name)
}

case class DeprecatedFunction(name: Identifier, deprecation: String) extends ErrorType {
  override def toString = "deprecated name: %s; %s".format(name, deprecation)
}

case object OperationOnUnrelatedSets extends ErrorType {
  override def toString = "cannot perform operation on unrelated sets"
}

case object UnionProvenanceDifferentLength extends ErrorType {
  override def toString = "cannot perform union on two sets with different numbers of identities"
}

case object IntersectProvenanceDifferentLength extends ErrorType {
  override def toString = "cannot perform intersect on two sets each with different numbers of identities"
}

case object DifferenceProvenanceDifferentLength extends ErrorType {
  override def toString = "cannot perform set difference on two sets each with different numbers of identities"
}

case object CondProvenanceDifferentLength extends ErrorType {
  override def toString = "cannot perform a conditional on sets each with different numbers of identities"
}

case object DifferenceWithNoCommonalities extends ErrorType {
  override def toString = "cannot perform set difference on two sets which have no commonality (always returns left)"
}

case object IntersectWithNoCommonalities extends ErrorType {
  override def toString = "cannot perform set intersection on two sets which have no commonality (always returns empty set)"
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

case object FunctionArgsInapplicable extends ErrorType {
  override def toString = "cannot apply function to specified arguments"
}

case object SolveLackingFreeVariables extends ErrorType {
  override def toString = "nothing to solve"
}

// intended to be a warning
case class UnusedLetBinding(id: Identifier) extends ErrorType {
  override def toString = "binding '%s' defined but not referenced in scope".format(id)
}

case class UnusedFormalBinding(id: Identifier) extends ErrorType {
  override def toString = "parameter '%s' defined but not referenced in scope".format(id)
}

case class UnusedTicVariable(id: TicId) extends ErrorType {
  override def toString = "function parameter %s defined but not referenced or constrained".format(id)
}

// intended to be a warning
case class UnableToSolveCriticalCondition(id: String) extends ErrorType {
  override def toString = "unable to solve for variable %s".format(id)
}

case object UnableToSolveCriticalConditionAnon extends ErrorType {
  override def toString = "unable to solve conditional: invalid expression type"
}

case class UnableToDetermineDefiningSet(id: String) extends ErrorType {
  override def toString = "unable to solve defining set for variable %s".format(id)
}

case object ConstraintsWithinInnerSolve extends ErrorType {
  override def toString = "cannot solve group set for constraints within a nested solve"
}

case object GroupSetInvolvingMultipleParameters extends ErrorType {
  override def toString = "cannot solve group set involving multiple variables"
}

case class InseparablePairedTicVariables(vars: Set[TicId]) extends ErrorType {
  override def toString = "cannot separate variables %s for group set".format(vars mkString ", ")
}

case class UnableToSolveTicVariable(tv: TicId) extends ErrorType {
  override def toString = "cannot solve variable %s".format(tv)
}

case object GroupTargetSetNotIndependent extends ErrorType {
  override def toString = "dependent target set for group conditional"
}

case object InvalidGroupConstraint extends ErrorType {
  override def toString = "solve constraint lacking variables"
}

case class ExtraVarsInGroupConstraint(id: TicId) extends ErrorType {
  override def toString = "invalid free variable usage (%s) in solve constraint".format(id)
}
