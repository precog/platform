package com.precog.quirrel

import scalaz.Tree

trait Phases {
  type Expr
  type Formal
  type Error

  type ConditionTree
  type GroupTree
  
  type Phase = Expr => Set[Error]
  
  private val Phases: List[Phase] =
    bindNames _ :: checkProvenance _ :: inferBuckets _ :: Nil
  
  protected def LoadId: Identifier
  protected def ExpandGlobId: Identifier
  protected def DistinctId: Identifier
  
  def bindNames(expr: Expr): Set[Error]
  def checkProvenance(expr: Expr): Set[Error]
  def inferBuckets(expr: Expr): Set[Error]
  
  def buildTrace(sigma: Map[Formal, Expr])(expr: Expr): Tree[(Map[Formal, Expr], Expr)]
  
  private[quirrel] def runPhasesInSequence(tree: Expr): Set[Error] =
    Phases.foldLeft(Set[Error]()) { _ ++ _(tree) }
  
  val Error: ErrorCompanion

  def showError(error: Error): String

  trait ErrorCompanion {
    def apply(node: Expr, tp: ErrorType): Error
    def unapply(err: Error): Option[ErrorType]
  }
}
