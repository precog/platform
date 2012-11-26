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
          case Root(_, CUndefined) => Root(loc, CUndefined)
          
          case Root(_, value) => {
            op match {
              case instructions.WrapArray =>    // TODO currently can't be a cvalue
                Operate(loc, op, child2)
              
              case _ => {
                val result = for {
                  col <- op1(op).f1(value)
                  if col isDefinedAt 0
                } yield col cValue 0
                
                Root(loc, result getOrElse CUndefined)
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
            case (Root(_, CUndefined), _) =>
              Some(Root(loc, CUndefined))
            
            case (_, Root(_, CUndefined)) =>
              Some(Root(loc, CUndefined))
            
            case (Root(_, leftValue), Root(_, rightValue)) => {
              val result = for {
                col <- op2.f2.partialLeft(leftValue).apply(rightValue)
                if col isDefinedAt 0
              } yield col cValue 0
              
              Some(Root(loc, result getOrElse CUndefined))
            }
            
            case _ => None
          }
        } getOrElse Join(loc, op, sort, left2, right2)
      }
      
      case Filter(loc, sort @ (CrossLeftSort | CrossRightSort), left, right) => {
        val left2 = recurse(left)
        val right2 = recurse(right)
        
        val back = (left2, right2) match {
          case (_, Root(_, CBoolean(true))) => Some(left2)
          case (_, Root(_, _)) => Some(Root(loc, CUndefined))
          case _ => None
        }
        
        back getOrElse Filter(loc, sort, left2, right2)
      }
    }}
  }
}
