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
package com.precog
package daze

import com.precog.yggdrasil._

import blueeyes.json._

trait StaticInlinerModule[M[+_]] extends DAG with EvaluatorMethodsModule[M] {
  import dag._
    
  trait StaticInliner extends EvaluatorMethods {
    def inlineStatics(graph: DepGraph, ctx: EvaluationContext, splits: Set[dag.Split]): DepGraph
  }
}

trait StdLibStaticInlinerModule[M[+_]] extends StaticInlinerModule[M] with StdLibModule[M] {
  import dag._
  import library._
  import instructions._

  trait StdLibStaticInliner extends StaticInliner {
    def inlineStatics(graph: DepGraph, ctx: EvaluationContext, splits: Set[dag.Split]): DepGraph = {
      graph.mapDown(
        recurse => {
          case graph @ Operate(op, child) => {
            recurse(child) match {
              case child2 @ Const(JUndefined) => Const(JUndefined)(child2.loc)

              case child2 @ Const(value) => {
                op match {
                  case instructions.WrapArray =>    // TODO currently can't be a cvalue
                    Operate(op, child2)(graph.loc)

                  case _ => {
                    val newOp1 = op1ForUnOp(op)
                    newOp1.fold(
                      _ => Operate(op, child2)(graph.loc),
                      newOp1 => {
                        val result = for {
                          // No Op1F1 that can be applied to a complex JValues
                          cvalue <- jValueToCValue(value)
                          col <- newOp1.f1(ctx).apply(cvalue)
                          if col isDefinedAt 0
                        } yield col jValue 0

                        Const(result getOrElse JUndefined)(graph.loc)
                      }
                    )
                  }
                }
              }

              case child2 => Operate(op, child2)(graph.loc)
            }
          }

          case graph @ Join(op, sort @ (CrossLeftSort | CrossRightSort), left, right) => {
            val left2 = recurse(left)
            val right2 = recurse(right)

            op2ForBinOp(op) flatMap { op2 =>
              (left2, right2) match {
                case (left2 @ Const(JUndefined), _) =>
                  Some(Const(JUndefined)(left2.loc))

                case (_, right2 @ Const(JUndefined)) =>
                  Some(Const(JUndefined)(right2.loc))

                case (left2 @ Const(leftValue), right2 @ Const(rightValue)) => {
                  val result = for {
                    // Noq Op2F2 that can be applied to a complex JValues
                    leftCValue <- jValueToCValue(leftValue)
                    rightCValue <- jValueToCValue(rightValue)
                    col <- op2.f2(ctx).partialLeft(leftCValue).apply(rightCValue)
                    if col isDefinedAt 0
                  } yield col jValue 0

                  Some(Const(result getOrElse JUndefined)(graph.loc))
                }

                case _ => None
              }
            } getOrElse Join(op, sort, left2, right2)(graph.loc)
          }

          case graph @ Filter(sort @ (CrossLeftSort | CrossRightSort), left, right) => {
            val left2 = recurse(left)
            val right2 = recurse(right)

            val back = (left2, right2) match {
              case (_, right2 @ Const(JTrue)) => Some(left2)
              case (_, right2 @ Const(_)) => Some(Const(JUndefined)(graph.loc))
              case _ => None
            }

            back getOrElse Filter(sort, left2, right2)(graph.loc)
          }
        },
        splits
      )
    }
  }
}
