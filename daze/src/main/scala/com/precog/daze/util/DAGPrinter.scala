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
package util

import bytecode._

trait DAGPrinter extends DAG {
  import instructions._
  import dag._
  
  def showDAG(root: DepGraph): String = {
    /* def loop(root: DepGraph, split: List[String]): String = root match {
      case SplitRoot(_, depth) => split.lift(depth) match {
        case Some(str) => "{%s}".format(str)
        case None => "<error>"
      }
      
      case Root(_, PushString(str)) => "\"%s\"".format(str)
      case Root(_, PushNum(num)) => num
      case Root(_, PushTrue) => "true"
      case Root(_, PushFalse) => "false"
      case Root(_, PushNull) => "null"
      case Root(_, PushObject) => "{}"
      case Root(_, PushArray) => "[]"
      
      case dag.New(_, parent) => loop(parent, split)
      
      case dag.LoadLocal(_, _, Root(_, PushString(path)), _) => path
      case dag.LoadLocal(_, _, _, _) => "<load>"
      
      case Operate(_, Neg, parent) => "~%s".format(loop(parent, split))
      case Operate(_, Comp, parent) => "!%s".format(loop(parent, split))
      case Operate(_, WrapArray, parent) => "[%s]".format(loop(parent, split))
      
      case dag.Reduce(_, red, parent) => "%s(%s)".format(showReduction(red), loop(parent, split))
      case dag.SetReduce(_, red, parent) => "%s(%s)".format(showSetReduction(red), loop(parent, split))
      
      case Join(_, IUnion, left, right) => "(%s iunion %s)".format(loop(left, split), loop(right, split))
      case Join(_, IIntersect, left, right) => "(%s iintersect %s)".format(loop(left, split), loop(right, split))

      case Join(_, SetDifference, left, right) => "(%s setDifference %s)".format(loop(left, split), loop(right, split))
      
      case Join(_, Map2Match(WrapObject), Root(_, PushString(property)), value) =>
        "{ %s: %s }".format(property, loop(value, split))
      
      case Join(_, Map2Cross(WrapObject), Root(_, PushString(property)), value) =>
        "{ %s: %s }".format(property, loop(value, split))
      
      case Join(_, Map2Cross(DerefObject), left, Root(_, PushString(prop))) =>
        "%s.%s".format(loop(left, split), prop)
      
      case Join(_, Map2Match(DerefArray), left, right) =>
        "%s[%s]".format(loop(left, split), loop(right, split))
      
      case Join(_, Map2Cross(DerefArray), left, right) =>
        "%s[%s]".format(loop(left, split), loop(right, split))
      
      case Join(_, Map2Match(op), left, right) => "(%s %s %s)".format(loop(left, split), showOp(op), loop(right, split))
      case Join(_, Map2Cross(op), left, right) => "(%s %s %s)".format(loop(left, split), showOp(op), loop(right, split))
      
      case dag.Filter(_, _, _, target, boolean) => "(%s where %s)".format(loop(target, split), loop(boolean, split))
      
      case dag.Split(_, parent, child) => loop(child, loop(parent, split) :: split)
    }
    
    loop(root, Nil) */
    
    sys.error("todo")
  }
  
  /* private def showReduction(red: Reduction) = red match {
    case Count => "count"
    case GeometricMean => "geometricMean"

    case Mean => "mean"
    case Median => "median"
    case Mode => "mode"
    
    case Max => "max"
    case Min => "min"
    
    case StdDev => "stdDev"
    case Sum => "sum"
    case SumSq => "sumSq"
    case Variance => "variance"
  }

  private def showSetReduction(red: SetReduction) = red match {
    case Distinct => "distinct"
  }
  
  private def showOp(op: BinaryOperation) = op match {
    case Add => "+"
    case Sub => "-"
    case Mul => "*"
    case Div => "/"
    
    case Lt => "<"
    case LtEq => "<="
    case Gt => ">"
    case GtEq => ">="
    
    case Eq => "="
    case NotEq => "!="
    
    case Or => "|"
    case And => "&"
    
    case JoinObject => "++"
    case JoinArray => "++"
    
    case ArraySwap => "swap"
    
    case DerefObject => "deref_object"
    case DerefArray => "deref_array"
    
    case WrapObject => "wrap_object"
  } */
}
