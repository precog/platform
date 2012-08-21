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
package typer

import com.codecommit.gll.LineStream

trait ForallRewriter extends parser.AST {
  import ast._

  def rewriteForall(tree: Expr): Expr = {
    val tree2 = rewrite(tree.root)
    bindRoot(tree2, tree2)
    tree2._errors appendFrom tree._errors

    tree2
  }

  def rewrite(tree: Expr): Expr = tree match {
    case Let(loc, name, params, left, right) => Let(loc, name, params, rewrite(left), rewrite(right))

    case Forall(loc, param, child) => {
      val name = Identifier(Vector(), "$forall")

      def loop(loc2: LineStream, params: Vector[TicId], tree: Expr): Expr = {
        tree match {
          case Forall(loc2, param2, child2) => loop(loc2, params :+ param2, child2) //todo case for Paren  - impossible case 
          case _ => Let(loc, name, params, tree, Dispatch(loc2, name, Vector.empty[Expr]))
        }
      }
      
      loop(loc, Vector(param), child)
    }

    case Import(loc, spec, child) => Import(loc, spec, rewrite(child))

    case New(loc, child) => New(loc, rewrite(child))

    case Relate(loc, from, to, in) => Relate(loc, rewrite(from), rewrite(to), rewrite(in))

    case t @ TicVar(loc, name) => t

    case s @ StrLit(loc, value) => s

    case n @ NumLit(loc, value) => n

    case b @ BoolLit(loc, value) => b

    case n @ NullLit(loc) => n

    case ObjectDef(loc, props) => {
      val mappedExprs = props map { case (str, expr) => (str, rewrite(expr)) }
      ObjectDef(loc, mappedExprs)
    }

    case ArrayDef(loc, values) => {
      val mappedExprs = values map { expr => rewrite(expr) }
      ArrayDef(loc, mappedExprs)
    }

    case Descent(loc, child, property) => Descent(loc, rewrite(child), property)
    
    case MetaDescent(loc, child, property) => MetaDescent(loc, rewrite(child), property)

    case Deref(loc, left, right) => Deref(loc, rewrite(left), rewrite(right))

    case Dispatch(loc, name, actuals) => {
      val mappedActuals = actuals map { expr => rewrite(expr) }
      Dispatch(loc, name, mappedActuals)
    }

    case Where(loc, left, right) => Where(loc, rewrite(left), rewrite(right))
    
    case With(loc, left, right) => With(loc, rewrite(left), rewrite(right))

    case Union(loc, left, right) => Union(loc, rewrite(left), rewrite(right))

    case Intersect(loc, left, right) => Intersect(loc, rewrite(left), rewrite(right))

    case Difference(loc, left, right) => Difference(loc, rewrite(left), rewrite(right))

    case Add(loc, left, right) => Add(loc, rewrite(left), rewrite(right))

    case Sub(loc, left, right) => Sub(loc, rewrite(left), rewrite(right))

    case Mul(loc, left, right) => Mul(loc, rewrite(left), rewrite(right))

    case Div(loc, left, right) => Div(loc, rewrite(left), rewrite(right))

    case Lt(loc, left, right) => Lt(loc, rewrite(left), rewrite(right))

    case LtEq(loc, left, right) => LtEq(loc, rewrite(left), rewrite(right))

    case Gt(loc, left, right) => Gt(loc, rewrite(left), rewrite(right))

    case GtEq(loc, left, right) => GtEq(loc, rewrite(left), rewrite(right))

    case Eq(loc, left, right) => Eq(loc, rewrite(left), rewrite(right))

    case NotEq(loc, left, right) => NotEq(loc, rewrite(left), rewrite(right))

    case And(loc, left, right) => And(loc, rewrite(left), rewrite(right))

    case Or(loc, left, right) => Or(loc, rewrite(left), rewrite(right))

    case Comp(loc, child) => Comp(loc, rewrite(child))

    case Neg(loc, child) => Neg(loc, rewrite(child))

    case Paren(loc, child) => Paren(loc, rewrite(child))

  }
}
