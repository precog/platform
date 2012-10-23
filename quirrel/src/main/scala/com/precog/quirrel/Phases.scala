package com.precog.quirrel

trait Phases {
  type Expr
  type Error

  type ConditionTree
  type GroupTree
  
  type Phase = Expr => Set[Error]
  
  private val Phases: List[Phase] =
    bindNames _ :: checkProvenance _ :: inferBuckets _ :: Nil
  
  protected def LoadId: Identifier
  protected def DistinctId: Identifier
  
  def bindNames(expr: Expr): Set[Error]
  def checkProvenance(expr: Expr): Set[Error]
  def inferBuckets(expr: Expr): Set[Error]
  
  def findCriticalConditions(expr: Expr): Map[TicId, Set[ConditionTree]]
  def findGroups(expr: Expr): Set[GroupTree]
  
  private[quirrel] def runPhasesInSequence(tree: Expr): Set[Error] =
    Phases.foldLeft(Set[Error]()) { _ ++ _(tree) }
  
  val Error: ErrorCompanion

  def showError(error: Error): String

  trait ErrorCompanion {
    def apply(node: Expr, tp: ErrorType): Error
    def unapply(err: Error): Option[ErrorType]
  }
}
