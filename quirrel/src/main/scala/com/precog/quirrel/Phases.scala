package com.precog.quirrel

import com.precog.util.BitSet

trait Phases {
  type Expr
  type Formal
  type Error

  type ConditionTree
  type GroupTree
  
  type Phase = Expr => Set[Error]

  type Sigma = Map[Formal, Expr]

  //todo ensure graph and indices have same length
  case class Trace(graph: Array[(Sigma, Expr)], indices: Array[BitSet])

  object Trace {
    val empty = Trace(Array.empty[(Sigma, Expr)], Array.empty[BitSet])
  }
  
  private val Phases: List[Phase] =
    bindNames _ :: checkProvenance _ :: inferBuckets _ :: Nil
  
  protected def LoadId: Identifier
  protected def ExpandGlobId: Identifier
  protected def DistinctId: Identifier
  
  def bindNames(expr: Expr): Set[Error]
  def checkProvenance(expr: Expr): Set[Error]
  def inferBuckets(expr: Expr): Set[Error]
  
  def buildTrace(sigma: Sigma)(expr: Expr): Trace
  
  private[quirrel] def runPhasesInSequence(tree: Expr): Set[Error] =
    Phases.foldLeft(Set[Error]()) { _ ++ _(tree) }
  
  val Error: ErrorCompanion

  def showError(error: Error): String

  trait ErrorCompanion {
    def apply(node: Expr, tp: ErrorType): Error
    def unapply(err: Error): Option[ErrorType]
  }
}
