/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.quirrel

import com.codecommit.gll.LineStream

trait Errors extends Phases {
  def isWarning(error: Error): Boolean
}

trait RawErrors extends Errors with Phases {
  type Error = ErrorType
  
  override def Error(node: Expr, tp: ErrorType): Error = tp
  
  def showError(error: Error) = error.toString
  
  override def isWarning(error: Error) = error match {
    case UnusedLetBinding(_) => true
    case UnableToSolveCriticalCondition(_) => true
    case _ => false
  }
}

trait LineErrors extends Errors with Phases with parser.AST {
  private val ErrorPattern = "error:%%d: %s%n    %%s%n    %%s"
  
  def showError(error: Error) = error.loc.formatError(ErrorPattern format error.tp)
  
  override def Error(node: Expr, tp: ErrorType) = Error(node.loc, tp)
  
  override def isWarning(error: Error) = error match {
    case Error(_, UnusedLetBinding(_)) => true
    case Error(_, UnableToSolveCriticalCondition(_)) => true
    case _ => false
  }
  
  case class Error(loc: LineStream, tp: ErrorType)
}


sealed trait ErrorType 

case class UndefinedTicVariable(name: TicId) extends ErrorType {
  override def toString = "undefined tic-variable: %s".format(name)
}

case class MultiplyDefinedTicVariable(name: TicId) extends ErrorType {
  override def toString = "tic-variable name used multiple times: %s".format(name)
}

case class UndefinedFunction(name: Identifier) extends ErrorType {
  override def toString = "undefined name: %s".format(name)
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

case object DifferenceProvenanceValue extends ErrorType {
  override def toString = "cannot perform set difference on values"
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

case object SolveLackingFreeVariables extends ErrorType {
  override def toString = "nothing to solve"
}

// intended to be a warning
case class UnusedLetBinding(id: Identifier) extends ErrorType {
  override def toString = "binding '%s' defined but not referenced in scope".format(id)
}

case class UnusedFormalBinding(id: Identifier) extends ErrorType {
  override def toString = "binding '%s' defined but not referenced in scope".format(id)
}

case class UnusedTicVariable(id: TicId) extends ErrorType {
  override def toString = "function parameter %s defined but not referenced or constrained".format(id)
}

// intended to be a warning
case class UnableToSolveCriticalCondition(id: String) extends ErrorType {
  override def toString = "unable to solve critical condition for function parameter %s".format(id)
}

case class UnableToDetermineDefiningSet(id: String) extends ErrorType {
  override def toString = "unable to solve defining set for function parameter %s".format(id)
}

case object GroupSetInvolvingMultipleParameters extends ErrorType {
  override def toString = "cannot solve group set involving multiple function parameters"
}

case class InseparablePairedTicVariables(vars: Set[TicId]) extends ErrorType {
  override def toString = "cannot separate function parameters %s for group set".format(vars mkString ", ")
}

case class UnableToSolveTicVariable(tv: TicId) extends ErrorType {
  override def toString = "cannot solve function parameter %s".format(tv)
}

case object GroupTargetSetNotIndependent extends ErrorType {
  override def toString = "dependent target set for group conditional"
}
