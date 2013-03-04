package com.precog.quirrel
package parser

import com.codecommit.gll.LineStream

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

  type CacheKey = String

  sealed abstract class Rule(val re: Regex)
  case class Keep(tpe: String, r: Regex) extends Rule(r)
  case class Ignore(r: Regex) extends Rule(r)

  case class Binding(tpe: String, name: String, value: String, pos: Int)

  object CacheKey {
    val spaceRe = """[ \n\r]+""".r
    val numRe = parser.numLiteralRegex
    val strRe = parser.strLiteralRegex
    val boolRe = """(?:true|false)(?!\b)""".r
    val commentRe = """--[^\n]+""".r
    //val commentRe2 = """\(-(?:[^-]|-[^\)])*-\)""".r
    val pathRe = parser.pathLiteralRegex
    val wordRe = """['a-zA-Z_]['a-zA-Z0-9_]*""".r

    val rules = Array(
      Ignore(commentRe),
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
                //println("appending data %s" format m)
                output.append(m)
                matched = true
                i += m.length
              }
            case Keep(tpe, re) =>
              re.findPrefixOf(input.substring(i)).foreach { m =>
                val name = nextName(tpe)
                bindings.append(Binding(tpe, name, m, i))
                //println("appending name %s" format name)
                output.append(name)
                matched = true
                i += m.length
              }
          }
          j += 1
        }
        if (!matched) {
          val c = input.charAt(i)
          //println("appending single %s" format c)
          output.append(c)
          i += 1
        }
      }
      (output.toString, bindings)
    }

    def fromExpr(original: String, expr: Expr): String = {
      def loop(expr: Expr): List[Literal] = expr match {
        case b: BoolLit => b :: Nil
        case n: NumLit => n :: Nil
        case s: StrLit => s :: Nil
        case node =>
          node.children.flatMap(loop)
      }

      val lits: List[Literal] = loop(expr).sortBy { lit =>
        (lit.loc.lineNum, lit.loc.colNum)
      }

      def buildLineLengths(): IndexedSeq[Int] = {
        val lens = mutable.ArrayBuffer.empty[Int]
        lens.append(0)
        var i = 0
        original.foreach { c =>
          i += 1
          if (c == '\n') lens.append(i)
        }
        lens.append(i)
        lens
      }
      val lens = buildLineLengths()

      def parseOriginal(lit: Literal, i: Int): (String, Int) = {
        val s = original.substring(i)
        lit match {
          case _: BoolLit =>
            if (s.startsWith("true")) ("b", 4) else if (s.startsWith("false")) ("b", 5) else {
              sys.error("error recovering boolean literal from %s (%s at %s)" format (s, original, i))
            }
          case _: NumLit =>
            parser.numLiteralRegex.findPrefixOf(s).map(x => ("n", x.length)).getOrElse {
              //sys.error("error recovering number literal from %s (%s at %s)" format (s, original, i))
              val msg = """
lens = %s


original = %s


expr = %s


i = %s
s = %s
""" format (lens.toList, original, expr, i, s)
              sys.error(msg)
            }
          case _: StrLit =>
            parser.pathLiteralRegex.findPrefixOf(s).map(x => ("p", x.length)).orElse {
              parser.strLiteralRegex.findPrefixOf(s).map(x => ("s", x.length))
            }.getOrElse {
              sys.error("error recovering string literal from %s (%s at %s)" format (s, original, i))
            }
        }
      }

      val sb = new StringBuilder(original.length)
      var counter = 1
      var last = 0
      var lastLoc: LineStream = null
      lits.foreach { lit =>
        if (lit.loc != lastLoc) {
          lastLoc = lit.loc

          val i = lens(lit.loc.lineNum - 1) + (lit.loc.colNum - 1)
          val s = try {
            original.substring(last, i)
          } catch {
            case e: Exception =>
              sys.error("for %s (lens=%s)... original.substring(%s, %s) blew up; original.length=%s" format (lit, lens(lit.loc.lineNum - 1), last, i, original.length))
          }
          sb.append(s)

          val (prefix, originalLength) = parseOriginal(lit, i)
          sb.append("$" + prefix + counter.toString)

          last = i + originalLength
          counter += 1
        }
      }
      sb.append(original.substring(last))

      sb.toString
    }
  }

  def findHoles(expr: Expr): List[Literal] = expr match {
    case b: BoolLit => b :: Nil
    case n: NumLit => n :: Nil
    case s: StrLit => s :: Nil
    case node => node.children.flatMap(findHoles)
  }

  def buildBindingIndex(expr: Expr): Map[Int, Int] = {
    val holes: List[(Literal, Int)] = findHoles(expr).zipWithIndex
    val sorted = holes.sortBy(h => (h._1.loc.lineNum, h._1.loc.colNum))
    sorted.map(_._2).zipWithIndex.toMap
  }

  def resolveBindings(expr: Expr, bindings: IndexedSeq[Binding]): Option[Expr] = {
    val index = buildBindingIndex(expr)
    val sortedBindings = bindings.zipWithIndex.map { case (b, i) =>
      (b, index(i))
    }.sortBy(_._2).map(_._1).toList

    replaceLiteralsS(expr, sortedBindings)
  }

  type BindingS[+A] = StateT[Option, List[Binding], A]

  def replaceLiteralsS(expr0: Expr, bindings: List[Binding]): Option[Expr] = {
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
        pop map { b => BoolLit(loc, b.value == "true") }

      case lit @ NumLit(loc, _) =>
        pop map { b => NumLit(loc, b.value) }

      case lit @ StrLit(loc, _) =>
        pop map { b => StrLit(loc, b.value) }

      case Let(loc, name, params, lchild0, rchild0) =>
        for (lchild <- repl(lchild0); rchild <- repl(rchild0))
          yield Let(loc, name, params, lchild, rchild)

      case Solve(loc, constraints0, child0) =>
        for {
          child <- repl(child0)
          constraints <- (constraints0 map repl).sequence
        } yield Solve(loc, constraints, child)

      case Import(loc, spec, child) =>
        repl(child) map (Import(loc, spec, _))

      case Assert(loc, pred0, child0) =>
        for (pred <- repl(pred0); child <- repl(child0))
          yield Assert(loc, pred, child)

      case Observe(loc, data0, samples0) =>
        for (data <- repl(data0); samples <- repl(data0))
          yield Observe(loc, data, samples)

      case New(loc, child0) =>
        repl(child0) map (New(loc, _))

      case Relate(loc, from0, to0, in0) =>
        for {
          in <- repl(in0)
          to <- repl(to0)
          from <- repl(from0)
        } yield Relate(loc, from, to, in)

      case TicVar(loc, name) =>
        TicVar(loc, name).point[BindingS]

      case UndefinedLit(loc) =>
        UndefinedLit(loc).point[BindingS]

      case NullLit(loc) =>
        NullLit(loc).point[BindingS]

      case ObjectDef(loc, props0) =>
        for {
          props <- (props0 map { case (prop, expr) =>
              repl(expr) map (prop -> _)
            }: Vector[BindingS[(String, Expr)]]).sequence
        } yield ObjectDef(loc, props)

      case ArrayDef(loc, values0) =>
        (values0 map repl).sequence map (ArrayDef(loc, _))

      case Descent(loc, child0, property) =>
        repl(child0) map (Descent(loc, _, property))

      case MetaDescent(loc, child0, property) =>
        repl(child0) map (MetaDescent(loc, _, property))

      case Deref(loc, lchild0, rchild0) =>
        for (lchild <- repl(lchild0); rchild <- repl(rchild0))
          yield Deref(loc, lchild, rchild)

      case Dispatch(loc, name, actuals) =>
        (actuals map repl).sequence map (Dispatch(loc, name, _))

      case Cond(loc, pred0, left0, right0) =>
        for {
          pred <- repl(pred0)
          left <- repl(left0)
          right <- repl(right0)
        } yield Cond(loc, pred, left, right)

      case Where(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Where(loc, left, right)

      case With(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield With(loc, left, right)

      case Union(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Union(loc, left, right)

      case Intersect(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Intersect(loc, left, right)

      case Difference(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Difference(loc, left, right)

      case Add(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Add(loc, left, right)

      case Sub(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Sub(loc, left, right)

      case Mul(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Mul(loc, left, right)

      case Div(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Div(loc, left, right)

      case Mod(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Mod(loc, left, right)

      case Pow(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Pow(loc, left, right)

      case Lt(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Lt(loc, left, right)

      case LtEq(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield LtEq(loc, left, right)

      case Gt(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Gt(loc, left, right)

      case GtEq(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield GtEq(loc, left, right)

      case Eq(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Eq(loc, left, right)

      case NotEq(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield NotEq(loc, left, right)

      case And(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield And(loc, left, right)

      case Or(loc, left0, right0) =>
        for (left <- repl(left0); right <- repl(right0))
          yield Or(loc, left, right)

      case Comp(loc, expr) =>
        repl(expr) map (Comp(loc, _))

      case Neg(loc, expr) =>
        repl(expr) map (Neg(loc, _))

      case Paren(loc, expr) =>
        repl(expr) map (Paren(loc, _))
    }

    val result = for {
      expr <- repl(expr0)
      _ <- StateT { s: List[Binding] =>
          s.isEmpty.option((Nil, ()))
        }: BindingS[Unit]
    } yield expr

    result.eval(bindings)
  }

  class Cache {
    private val cache: mutable.Map[String, Expr] = mutable.Map.empty

    def getOrElseUpdate(query: LineStream)(f: LineStream => Set[Expr]): Set[Expr] = {
      val s = query.toString
      val (key, bindings) = CacheKey.fromString(s)
      cache.get(key).flatMap { expr =>
        resolveBindings(expr, bindings) map { root =>
          bindRoot(root, root)
          root
        }
      } map { expr =>
        Set(expr)
      } getOrElse {
        val exprs = f(query)
        if (exprs.size == 1) {
          val value = exprs.head
          val key2 = CacheKey.fromExpr(s, value)
          // TODO: need to store loc or line info?
          cache(key2) = value
        }
        exprs
      }
    }
  }
}
