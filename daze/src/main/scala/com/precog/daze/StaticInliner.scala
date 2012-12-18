package com.precog
package daze

import com.precog.yggdrasil._

trait StaticInliner[M[+_]] extends DAG with EvaluatorMethods[M] {
  import dag._
  import instructions._
  
  def inlineStatics(graph: DepGraph, ctx: EvaluationContext): DepGraph = {
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
                  col <- op1(op).f1(ctx).apply(value)
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
                col <- op2.f2(ctx).partialLeft(leftValue).apply(rightValue)
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
