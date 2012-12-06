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

trait StaticInliner[M[+_]] extends DAG with EvaluatorMethods[M] {
  import dag._
  import instructions._
  
  def inlineStatics(graph: DepGraph): DepGraph = {
    graph mapDown { recurse => {
      case Operate(loc, op, child) => {
        val child2 = recurse(child)
        
        
        child2 match {
          case Const(_, CUndefined) => Const(loc, CUndefined)
          
          case Const(_, value) => {
            op match {
              case instructions.WrapArray =>    // TODO currently can't be a cvalue
                Operate(loc, op, child2)
              
              case _ => {
                val result = for {
                  col <- op1(op).f1(value)
                  if col isDefinedAt 0
                } yield col cValue 0
                
                Const(loc, result getOrElse CUndefined)
              }
            }
          }
          
          case _ => Operate(loc, op, child2)
        }
      }
      
      case Join(loc, op, sort @ (CrossLeftSort | CrossRightSort), left, right) => {
        val left2 = recurse(left)
        val right2 = recurse(right)
        
        op2ForBinOp(op) flatMap { op2 =>
          (left2, right2) match {
            case (Const(_, CUndefined), _) =>
              Some(Const(loc, CUndefined))
            
            case (_, Const(_, CUndefined)) =>
              Some(Const(loc, CUndefined))
            
            case (Const(_, leftValue), Const(_, rightValue)) => {
              val result = for {
                col <- op2.f2.partialLeft(leftValue).apply(rightValue)
                if col isDefinedAt 0
              } yield col cValue 0
              
              Some(Const(loc, result getOrElse CUndefined))
            }
            
            case _ => None
          }
        } getOrElse Join(loc, op, sort, left2, right2)
      }
      
      case Filter(loc, sort @ (CrossLeftSort | CrossRightSort), left, right) => {
        val left2 = recurse(left)
        val right2 = recurse(right)
        
        val back = (left2, right2) match {
          case (_, Const(_, CBoolean(true))) => Some(left2)
          case (_, Const(_, _)) => Some(Const(loc, CUndefined))
          case _ => None
        }
        
        back getOrElse Filter(loc, sort, left2, right2)
      }
    }}
  }
}
