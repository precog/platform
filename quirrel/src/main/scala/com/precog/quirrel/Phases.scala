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

import com.precog.util.BitSet

trait Phases {
  type Expr
  type Formal
  type Error

  type ConditionTree
  type GroupTree
  
  type Phase = Expr => Set[Error]

  type Sigma = Map[Formal, Expr]

  // if `indices(i)(j)` is set, then there is a graph arrow from `nodes(i)` to `nodes(j)`
  case class Trace(nodes: Array[(Sigma, Expr)], indices: Array[BitSet])

  object Trace {
    val empty = Trace(Array.empty[(Sigma, Expr)], Array.empty[BitSet])

    def safeCopy(trace: Trace, node: (Sigma, Expr), indices: BitSet) =
      trace.copy(
        nodes = trace.nodes :+ node,
        indices = trace.indices :+ indices) 
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
