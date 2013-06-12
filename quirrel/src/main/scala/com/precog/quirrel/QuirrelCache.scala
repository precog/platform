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
package parser

import com.precog.util.cache._
import com.precog.util._

import com.codecommit.gll._

import scala.collection.mutable
import scala.util.matching.Regex

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._
import scalaz.std.boolean._
import scalaz.std.option._
import scalaz.std.vector._

trait QuirrelCache extends AST { parser: Parser =>
  import ast._

  private sealed abstract class Rule(val re: Regex)
  private case class Keep(tpe: String, r: Regex) extends Rule(r)
  private case class Ignore(r: Regex) extends Rule(r)

  private case class Slot(lineNum: Int, colNum: Int, width: Int)
  private case class Binding(tpe: String, name: String, rawValue: String, pos: Int) {
    def value: String = tpe match {
      case "p" => canonicalizePath(rawValue)
      case "s" => canonicalizeStr(rawValue)
      case _ => rawValue
    }
  }

  private type CacheKey = String
  private type CacheValue = (Expr, Map[String, Slot])

  private object CacheKey {
    val spaceRe = parser.whitespace
    val numRe = parser.numLiteralRegex
    val strRe = parser.strLiteralRegex
    val boolRe = """(?:true|false)(?!\B)""".r
    //val commentRe = """--[^\n]*""".r
    //val commentRe2 = """\(-(?:[^-]|-[^\)])*-\)""".r
    val pathRe = parser.pathLiteralRegex
    val wordRe = """['a-zA-Z_]['a-zA-Z0-9_]*""".r

    val rules = Array(
      // Ignore(commentRe),
      Ignore(spaceRe),
      Keep("n", numRe),
      Keep("b", boolRe),
      Keep("s", strRe),
      Keep("p", pathRe),
      Ignore(wordRe)
    )

    def fromString(input: String): (String, IndexedSeq[Binding]) = {
      var counter = 1
      def nextName(tpe: String): String = {
        val name = "$" + tpe + counter.toString
        counter += 1
        name
      }

      val len = input.length
      val bindings = mutable.ArrayBuffer.empty[Binding]
      val output = new StringBuilder(input.length)

      var i = 0
      while (i < len) { 
        var j = 0
        var matched = false
        while (!matched && j < rules.length) {
          rules(j) match {
            case Ignore(re) =>
              re.findPrefixOf(input.substring(i)).foreach { m =>
                output.append(m)
                matched = true
                i += m.length
              }
            case Keep(tpe, re) =>
              re.findPrefixOf(input.substring(i)).foreach { m =>
                val name = nextName(tpe)
                bindings.append(Binding(tpe, name, m, i))
                output.append(name)
                matched = true
                i += m.length
              }
          }
          j += 1
        }
        if (!matched) {
          val c = input.charAt(i)
          output.append(c)
          i += 1
        }
      }
      (output.toString, bindings)
    }

    def fromExpr(original: String, expr: Expr): (CacheKey, Map[String, Slot]) = {
      val lits: List[Literal] = findHoles(expr).sortBy { lit =>
        (lit.loc.lineNum, lit.loc.colNum)
      }

      def buildLineLengths(): IndexedSeq[Int] = {
        val lens = mutable.ArrayBuffer.empty[Int]
        lens.append(0)
        var i = 0
        while (i < original.length) {
          val c = original.charAt(i)
          i += 1
          if (c == '\n') lens.append(i)
        }
        lens.append(i)
        lens
      }
      val lens = buildLineLengths()

      def parseLiteral(lit: Literal, i: Int): (String, Int) = {
        val s = original.substring(i)
        lit match {
          case _: BoolLit =>
            if (s.startsWith("true")) ("b", 4) else if (s.startsWith("false")) ("b", 5) else {
              sys.error("error recovering boolean literal from %s (%s at %s)" format (s, original, i))
            }
          case _: NumLit =>
            parser.numLiteralRegex.findPrefixOf(s).map(x => ("n", x.length)).getOrElse {
              sys.error("error recovering number literal from %s (%s at %s)" format (s, original, i))
            }
          case _: StrLit =>
            parser.pathLiteralRegex.findPrefixOf(s).map(x => ("p", x.length)).orElse {
              parser.relPathLiteralRegex.findPrefixOf(s).map(x => ("rp", x.length)).orElse {
                parser.strLiteralRegex.findPrefixOf(s).map(x => ("s", x.length))
              }
            }.getOrElse {
              sys.error("error recovering string literal from %s (%s at %s)" format (s, original, i))
            }
        }
      }

      val slots = mutable.Map.empty[String, Slot]
      val sb = new StringBuilder(original.length)
      var counter = 1
      var last = 0
      var lastLoc: LineStream = null
      lits.foreach { lit =>
        if (lit.loc != lastLoc) {
          lastLoc = lit.loc

          val i = lens(lit.loc.lineNum - 1) + (lit.loc.colNum - 1)
          val s = original.substring(last, i)
          sb.append(s)

          val (prefix, width) = parseLiteral(lit, i)
          val name = "$" + prefix + counter.toString
          sb.append(name)
          slots(name) = Slot(lit.loc.lineNum, lit.loc.colNum, width)

          last = i + width
          counter += 1
        }
      }
      sb.append(original.substring(last))

      (sb.toString, slots.toMap)
    }
  }

  def findHoles(expr: Expr): List[Literal] = expr match {
    
    // These are the kinds of literals we are interested in
    // (those whose values vary).
    case b: BoolLit => b :: Nil
    case n: NumLit => n :: Nil
    case s: StrLit => s :: Nil

    // Let#children does not return all the children, so we will
    // handle it specially.
    case Let(_, _, _, c1, c2) =>
      findHoles(c1) ++ findHoles(c2)
    
    case node => node.children.flatMap(findHoles)
  }

  def buildBindingIndex(expr: Expr): Map[Int, Int] = {
    val holes: List[(Literal, Int)] = findHoles(expr).zipWithIndex
    val sorted = holes.sortBy(h => (h._1.loc.lineNum, h._1.loc.colNum))
    sorted.map(_._2).zipWithIndex.toMap
  }

  def resolveBindings(expr: Expr, bindings: IndexedSeq[Binding],
      slots: Map[String, Slot]): Option[Expr] = {
    val index = buildBindingIndex(expr)
    val sortedBindings = bindings.zipWithIndex.map { case (b, i) =>
      (b, index(i))
    }.sortBy(_._2).map(_._1).toList

    val result = replaceLiteralsS(expr, sortedBindings, locUpdates(bindings, slots))
    result
  }

  def locUpdates(bindings: IndexedSeq[Binding], slots: Map[String, Slot]): LineStream => LineStream = {
    val widths: Map[String, Int] = bindings.map { b =>
      (b.name, b.rawValue.length)
    }.toMap

    val deltas: Map[Int, List[(Int, Int)]] = slots.toList.map {
      case (name, Slot(lineNum, colNum, oldWidth)) =>
        val width = widths(name)
        val delta = width - oldWidth
        lineNum -> (colNum, delta)
      }.groupBy(_._1).map { case (lineNum, ds) =>
        (lineNum, ds.map(_._2).sortBy(_._1))
      }

    { (loc: LineStream) =>
      val colNum = deltas get loc.lineNum map { ds =>
        ds.takeWhile(_._1 < loc.colNum).map(_._2).sum
      } map (_ + loc.colNum) getOrElse loc.colNum

      loc match {
        case ln: LazyLineCons =>
          new LazyLineCons(ln.head, ln.tail, ln.line, ln.lineNum, colNum)
        case ln: StrictLineCons =>
          new StrictLineCons(ln.head, ln.tail, ln.line, ln.lineNum, colNum)
        case LineNil =>
          LineNil
      }
    }
  }

  private type BindingS[+A] = StateT[Option, List[Binding], A]

  private def replaceLiteralsS(expr0: Expr, bindings: List[Binding], updateLoc: LineStream => LineStream): Option[Expr] = {
    implicit val stateM = StateT.stateTMonadState[List[Binding], Option]
    import stateM._

    def pop: BindingS[Binding] = StateT {
      case binding :: bindings =>
        Some((bindings, binding))
      case Nil =>
        None
    }

    def repl(expr: Expr): BindingS[Expr] = expr match {
      case lit @ BoolLit(loc, _) =>
        pop map { b => BoolLit(updateLoc(loc), b.value == "true") }

      case lit @ NumLit(loc, _) =>
        pop map { b => NumLit(updateLoc(loc), b.value) }

      case lit @ StrLit(loc, _) =>
        pop map { b => StrLit(updateLoc(loc), b.value) }

      case Let(loc, name, params, lchild0, rchild0) =>
        for (lchild <- repl(lchild0); rchild <- repl(rchild0))
          yield Let(updateLoc(loc), name, params, lchild, rchild)

      case Solve(loc, constraints0, child0) =>
        for {
          child <- repl(child0)
          constraints <- (constraints0 map repl).sequence
        } yield Solve(updateLoc(loc), constraints, child)

      case Import(loc, spec, child) =>
        repl(child) map (Import(updateLoc(loc), spec, _))

      case Assert(loc, pred0, child0) =>
        for (pred <- repl(pred0); child <- repl(child0))
          yield Assert(updateLoc(loc), pred, child)

      case Observe(loc, data0, samples0) =>
        for (data <- repl(data0); samples <- repl(data0))
          yield Observe(updateLoc(loc), data, samples)

      case New(loc, child0) =>
        repl(child0) map (New(updateLoc(loc), _))

      case Relate(loc, from0, to0, in0) =>
        for {
          in <- repl(in0)
          to <- repl(to0)
          from <- repl(from0)
        } yield Relate(updateLoc(loc), from, to, in)

      case TicVar(loc, name) =>
        TicVar(updateLoc(loc), name).point[BindingS]

      case UndefinedLit(loc) =>
        UndefinedLit(updateLoc(loc)).point[BindingS]

      case NullLit(loc) =>
        NullLit(updateLoc(loc)).point[BindingS]

      case ObjectDef(loc, props0) =>
        for {
          props <- (props0 map { case (prop, expr) =>
              repl(expr) map (prop -> _)
            }: Vector[BindingS[(String, Expr)]]).sequence
        } yield ObjectDef(updateLoc(loc), props)

      case ArrayDef(loc, values0) =>
        (values0 map repl).sequence map (ArrayDef(updateLoc(loc), _))

      case Descent(loc, child0, property) =>
        repl(child0) map (Descent(updateLoc(loc), _, property))

      case MetaDescent(loc, child0, property) =>
        repl(child0) map (MetaDescent(updateLoc(loc), _, property))

      case Deref(loc, lchild0, rchild0) =>
        for (lchild <- repl(lchild0); rchild <- repl(rchild0))
          yield Deref(updateLoc(loc), lchild, rchild)

      case Dispatch(loc, name, actuals) =>
        (actuals map repl).sequence map (Dispatch(updateLoc(loc), name, _))

      case Cond(loc, pred0, left0, right0) =>
        for {
          pred <- repl(pred0)
          left <- repl(left0)
          right <- repl(right0)
        } yield Cond(updateLoc(loc), pred, left, right)

      case Where(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Where(updateLoc(loc), left, right)

      case With(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield With(updateLoc(loc), left, right)

      case Union(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Union(updateLoc(loc), left, right)

      case Intersect(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Intersect(updateLoc(loc), left, right)

      case Difference(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Difference(updateLoc(loc), left, right)

      case Add(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Add(updateLoc(loc), left, right)

      case Sub(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Sub(updateLoc(loc), left, right)

      case Mul(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Mul(updateLoc(loc), left, right)

      case Div(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Div(updateLoc(loc), left, right)

      case Mod(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Mod(updateLoc(loc), left, right)

      case Pow(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Pow(updateLoc(loc), left, right)

      case Lt(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Lt(updateLoc(loc), left, right)

      case LtEq(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield LtEq(updateLoc(loc), left, right)

      case Gt(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Gt(updateLoc(loc), left, right)

      case GtEq(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield GtEq(updateLoc(loc), left, right)

      case Eq(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Eq(updateLoc(loc), left, right)

      case NotEq(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield NotEq(updateLoc(loc), left, right)

      case And(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield And(updateLoc(loc), left, right)

      case Or(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Or(updateLoc(loc), left, right)

      case Comp(loc, expr) =>
        repl(expr) map (Comp(updateLoc(loc), _))

      case Neg(loc, expr) =>
        repl(expr) map (Neg(updateLoc(loc), _))

      case Paren(loc, expr) =>
        repl(expr) map (Paren(updateLoc(loc), _))
    }

    val result = for {
      expr <- repl(expr0)
      _ <- StateT { s: List[Binding] =>
          s.isEmpty.option((Nil, ()))
        }: BindingS[Unit]
    } yield expr

    result.eval(bindings)
  }

  class ParseCache(maxSize: Long) {
    private val cache: mutable.Map[CacheKey, CacheValue] = Cache.simple(Cache.MaxSize(maxSize))

    def getOrElseUpdate(query: LineStream)(f: LineStream => Set[Expr]): Set[Expr] = {
      val s = query.toString
      val (key, bindings) = CacheKey.fromString(s)
      cache.get(key).flatMap { case (expr, slots) =>
        resolveBindings(expr, bindings, slots) map { root =>
          bindRoot(root, root)
          root
        }
      } map { expr =>
        Set(expr)
      } getOrElse {
        val exprs = f(query)
        if (exprs.size == 1) {
          val value = exprs.head
          val (key2, slots) = CacheKey.fromExpr(s, value)
          cache(key2) = (value, slots)
        }
        exprs
      }
    }
  }
}
