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

import com.codecommit.gll.ExpectedLiteral
import com.codecommit.gll.ExpectedRegex
import com.codecommit.gll.Failure
import com.codecommit.gll.LineStream
import com.codecommit.gll.RegexParsers
import com.codecommit.gll.RegexUtils
import com.codecommit.gll.Result
import com.codecommit.gll.Success
import com.codecommit.gll.UnexpectedEndOfStream
import com.codecommit.gll.ast.{UnaryNode, Filters, Node}
import com.codecommit.util.ComplementarySet
import com.codecommit.util.UniversalCharSet

trait Parser extends RegexParsers with Filters with AST {
  import ast._
  
  def parse(str: String): Set[Expr] =
    parse(LineStream(str))
  
  def parse(input: LineStream): Set[Expr] = {
    val results = expr(input)
    val successes = results collect { case Success(tree, _) => tree }
    val failures = results collect { case f: Failure => f }
    
    if (successes.isEmpty)
      handleFailures(failures)
    else
      handleSuccesses(successes)
  }
  
  // TODO these functions need to be privatized
  def handleSuccesses(forest: Stream[Expr]): Set[Expr] = {
    val back = forest map { root =>
      bindRoot(root, root)
      root
    }
    
    Set(back: _*)
  }
  
  def handleFailures(forest: Stream[Failure]): Nothing = {
    val sorted = forest.toList sortWith { _.tail.length < _.tail.length }
    val length = sorted.head.tail.length
    
    val failures = Set(sorted takeWhile { _.tail.length == length }: _*)
    throw ParseException(failures)
  }
  
  // %%
  
  lazy val expr: Parser[Expr] = (
      id ~ "(" ~ formals ~ ")" ~ ":=" ~ expr ~ expr ^# { (loc, id, _, fs, _, _, e1, e2) => 
        Let(loc, Identifier(Vector(), id), fs, e1, e2)
      }
    | id ~ ":=" ~ expr ~ expr                       ^# { (loc, id, _, e1, e2) =>
        Let(loc, Identifier(Vector(), id), Vector(), e1, e2)
      }

    | """solve\b""".r ~ actuals ~ expr ^# { (loc, _, t, e) => Solve(loc, t, e) }

    | """observe\b""".r ~ "(" ~ expr ~ "," ~ expr ~ ")" ^# { (loc, _, _, e1, _, e2, _) => Observe(loc, e1, e2) }
    
    | """import\b""".r ~ importSpec ~ expr ^# { (loc, _, s, e) => Import(loc, s, e) }
    
    | """assert\b""".r ~ expr ~ expr ^# { (loc, _, e1, e2) => Assert(loc, e1, e2) }
    
    | """new\b""".r ~ expr ^# { (loc, _, e) => New(loc, e) }
    | relations ~ expr     ^# { (loc, es, e) => buildDeepRelate(loc, es, e) }
    
    | namespacedId ^# { (loc, id) => Dispatch(loc, id, Vector()) }  
    | ticId        ^# TicVar
    
    | pathLiteral ^# { (loc, str) =>
      Dispatch(loc, LoadId, Vector(
        Dispatch(loc, ExpandGlobId, Vector(StrLit(loc, str)))))
    }
    
    | relPathLiteral ^# { (loc, str) =>
      Dispatch(loc, RelLoadId, Vector(
        Dispatch(loc, ExpandGlobId, Vector(StrLit(loc, str)))))
    }
    
    | strLiteral  ^# StrLit
    | numLiteral  ^# NumLit
    | boolLiteral ^# BoolLit
    | undefinedLiteral ^# { (loc, _) => UndefinedLit(loc) }
    | nullLiteral ^# { (loc, _) => NullLit(loc) }
    
    | "{" ~ properties ~ "}"      ^# { (loc, _, ps, _) => ObjectDef(loc, ps) }
    | "[" ~ nullableActuals ~ "]" ^# { (loc, _, as, _) => ArrayDef(loc, as) }
    
    | expr ~ "." ~ propertyName   ^# { (loc, e, _, p) => Descent(loc, e, p) }
    | expr ~ "@" ~ propertyName   ^# { (loc, e, _, p) => MetaDescent(loc, e, p) }
    | expr ~ "[" ~ expr ~ "]"     ^# { (loc, e1, _, e2, _) => Deref(loc, e1, e2) }
    
    | namespacedId ~ "(" ~ actuals ~ ")" ^# { (loc, id, _, as, _) => Dispatch(loc, id, as) }  

    | """if\b""".r ~ expr ~ """then\b""".r ~ expr ~ """else\b""".r ~ expr ^# { (loc, _, e1, _, e2, _, e3) => Cond(loc, e1, e2, e3) }

    | expr ~ """where\b""".r ~ expr      ^# { (loc, e1, _, e2) => Where(loc, e1, e2) }
    | expr ~ """with\b""".r ~ expr       ^# { (loc, e1, _, e2) => With(loc, e1, e2) }
    | expr ~ """union\b""".r ~ expr      ^# { (loc, e1, _, e2) => Union(loc, e1, e2) }
    | expr ~ """intersect\b""".r ~ expr  ^# { (loc, e1, _, e2) => Intersect(loc, e1, e2) }
    | expr ~ """difference\b""".r ~ expr ^# { (loc, e1, _, e2) => Difference(loc, e1, e2) }
    
    | expr ~ "+" ~ expr ^# { (loc, e1, _, e2) => Add(loc, e1, e2) }
    | expr ~ "-" ~ expr ^# { (loc, e1, _, e2) => Sub(loc, e1, e2) }
    | expr ~ "*" ~ expr ^# { (loc, e1, _, e2) => Mul(loc, e1, e2) }
    | expr ~ "/" ~ expr ^# { (loc, e1, _, e2) => Div(loc, e1, e2) }
    | expr ~ "%" ~ expr ^# { (loc, e1, _, e2) => Mod(loc, e1, e2) }
    | expr ~ "^" ~ expr ^# { (loc, e1, _, e2) => Pow(loc, e1, e2) }
    
    | expr ~ "<" ~ expr  ^# { (loc, e1, _, e2) => Lt(loc, e1, e2) }
    | expr ~ "<=" ~ expr ^# { (loc, e1, _, e2) => LtEq(loc, e1, e2) }
    | expr ~ ">" ~ expr  ^# { (loc, e1, _, e2) => Gt(loc, e1, e2) }
    | expr ~ ">=" ~ expr ^# { (loc, e1, _, e2) => GtEq(loc, e1, e2) }
    
    | expr ~ "=" ~ expr  ^# { (loc, e1, _, e2) => Eq(loc, e1, e2) }
    | expr ~ "!=" ~ expr ^# { (loc, e1, _, e2) => NotEq(loc, e1, e2) }
    
    | expr ~ "&" ~ expr ^# { (loc, e1, _, e2) => And(loc, e1, e2) }
    | expr ~ "|" ~ expr ^# { (loc, e1, _, e2) => Or(loc, e1, e2) }
    
    | "!" ~ expr           ^# { (loc, _, e) => Comp(loc, e) }
    | """neg\b""".r ~ expr ^# { (loc, _, e) => Neg(loc, e) }
    
    | "(" ~ expr ~ ")" ^# { (loc, _, e, _) => Paren(loc, e) }
  ) filter (precedence & arrayDefDeref & relateRelate)
  
  private lazy val importSpec: Parser[ImportSpec] = (
      namespace ~ "::" ~ "*" ^^ { (p, _, _) => WildcardImport(p) }
    | namespace              ^^ SpecificImport
  )
  
  private lazy val namespacedId: Parser[Identifier] = (
      namespace ~ "::" ~ id ^^ { (ns, _, id) => Identifier(ns, id) }
    | id                    ^^ { str => Identifier(Vector(), str) }
  )

  private lazy val namespace: Parser[Vector[String]] = (
      namespace ~ "::" ~ id ^^ { (ns, _, id) => ns :+ id }
    | id                    ^^ { Vector(_) }
  )

  private lazy val formals: Parser[Vector[String]] = (
      formals ~ "," ~ id ^^ { (fs, _, f) => fs :+ f }
    | id                 ^^ { Vector(_) }
  )
  
  private lazy val relations: Parser[Vector[Expr]] = (
      relations ~ "~" ~ expr ^^ { (es, _, e) => es :+ e }
    | expr ~ "~" ~ expr      ^^ { (e1, _, e2) => Vector(e1, e2) }
  )
  
  private lazy val actuals: Parser[Vector[Expr]] = (
      actuals ~ "," ~ expr ^^ { (es, _, e) => es :+ e }
    | expr                 ^^ { Vector(_) }
  )
  
  private lazy val nullableActuals = (
      actuals
    | ""      ^^^ Vector[Expr]()
  )
  
  private lazy val properties: Parser[Vector[(String, Expr)]] = (
      properties ~ "," ~ property ^^ { (ps, _, p) => ps :+ p }
    | property                    ^^ { Vector(_) }
    | ""                          ^^^ Vector()
  )
  
  private lazy val property = propertyName ~ ":" ~ expr ^^ { (n, _, e) => (n, e) }
  
  private lazy val id = """[a-zA-Z]['a-zA-Z_0-9]*|_['a-zA-Z_0-9]+""".r \ keywords
  
  private lazy val ticId = """'[a-zA-Z_0-9]['a-zA-Z_0-9]*""".r
  
  private lazy val propertyName = (
      """[a-zA-Z_][a-zA-Z_0-9]*""".r
    | """`([^`\\]|\\.)+`""".r ^^ canonicalizePropertyName
    | """"([^"\\]|\\.)+"""".r ^^ canonicalizeStr        //"
  )
  
  private val basePathLiteralRegex = """(/[a-zA-Z0-9\-\._~:/?#@!$&'*+=]+)+"""
  
  private[quirrel] lazy val pathLiteralRegex = ("/" + basePathLiteralRegex).r
  private[quirrel] lazy val relPathLiteralRegex = ("""\.""" + basePathLiteralRegex).r
  
  private lazy val pathLiteral = (pathLiteralRegex preferred) ^^ canonicalizePath
  private lazy val relPathLiteral = (relPathLiteralRegex preferred) ^^ canonicalizeRelPath 
  
  private[quirrel] lazy val strLiteralRegex = """"([^\n\r\\"]|\\.)*"""".r //"
  private lazy val strLiteral = strLiteralRegex ^^ canonicalizeStr
  
  private[quirrel] lazy val numLiteralRegex = """[0-9]+(\.[0-9]+)?([eE][0-9]+)?""".r
  private lazy val numLiteral = numLiteralRegex
  
  private lazy val boolLiteral: Parser[Boolean] = (
      "true"  ^^^ true
    | "false" ^^^ false
  )

  private lazy val undefinedLiteral = """undefined\b""".r
  
  private lazy val nullLiteral = """null\b""".r

  private lazy val keywords = "new|true|false|where|with|union|intersect|difference|neg|undefined|null|import|solve|if|then|else|assert|observe".r
  
  override val whitespace = """([;\s]+|--.*|\(-([^\-]|-+[^)\-])*-+\))+""".r
  override val skipWhitespace = true
  
  private val precedence =
    prec(
      (Descent, MetaDescent),
      Deref,
      Comp,
      Neg,
      Pow,
      (Mul, Div, Mod),
      (Add, Sub),
      (Lt, LtEq, Gt, GtEq),
      (Eq, NotEq),
      (And, Or),
      (Union, Intersect, Difference),
      Cond,
      With,
      New,
      Where,
      Observe,
      (Relate, Let, Solve, Import, Assert))
      
  private def arrayDefDeref = new com.codecommit.gll.ast.Filter[Node] {
    def apply(n: Node): Boolean = n match {
      case n if n.getClass == classOf[Deref] =>
        n.children.head.getClass != classOf[ArrayDef]
      
      case _ => true
    } 
  }
  
  private def relateRelate = new com.codecommit.gll.ast.Filter[Node] {
    def apply(n: Node): Boolean = n match {
      
      // desugared due to some sort of wonky scalac bug that I couldn't minimize...
      case n if n.isInstanceOf[Relate] =>
        !n.asInstanceOf[Relate].from.isInstanceOf[Relate] &&
          !n.asInstanceOf[Relate].to.isInstanceOf[Relate]
      
      case _ => true
    }
  }
  
  // %% 
  
  private def buildDeepRelate(loc: LineStream, relations: Vector[Expr], e: Expr): Expr = {
    val builders = relations zip (relations drop 1) map {
      case (e1, e2) => { e3: Expr => Relate(loc, e1, e2, e3) }
    }
    
    builders.foldRight(e) { _(_) }
  }
  
  private[quirrel] def canonicalizeStr(str: String): String = {
    val (back, _) = str.foldLeft(("", false)) {
      case ((acc, false), '\\') => (acc, true)
      case ((acc, false), c) => (acc + c, false)
      
      case ((acc, true), 'n') => (acc + '\n', false)
      case ((acc, true), 'r') => (acc + '\r', false)
      case ((acc, true), 'f') => (acc + '\f', false)
      case ((acc, true), 't') => (acc + '\t', false)
      case ((acc, true), '0') => (acc + '\0', false)
      case ((acc, true), '\\') => (acc + '\\', false)
      case ((acc, true), c) => (acc + c, false)
    }
    
    back.substring(1, back.length - 1)
  }
  
  /**
   * Identity function
   */
  private[quirrel] def canonicalizePath(str: String): String = str substring 1
  private[quirrel] def canonicalizeRelPath(str: String): String = str substring 2
  
  private def canonicalizePropertyName(str: String): String = {
    val (back, _) = str.substring(1, str.length - 1).foldLeft(("", false)) {
      case ((acc, false), '\\') => (acc, true)
      case ((acc, _), c) => (acc + c, false)
    }
    
    back
  }
  
  case class ParseException(failures: Set[Failure]) extends RuntimeException {
    def mkString = {
      val tail = failures.head.tail      // if we have an empty set of failures, that's bad
      val result = ParseException reduceFailures failures
      tail formatError ("error:%d: " + result + "%n  %s%n  %s")
    }
    
    override def getMessage = ParseException reduceFailures failures
  }
  
  object ParseException extends (Set[Failure] => ParseException) {
    private val ExpectedPattern = "expected %s"
    private val SyntaxPattern = "syntax error"
    
    private lazy val Parsers = Map(expr -> "expression", property -> "property",
      pathLiteral -> "path", id -> "identifier", regex(ticId) -> "tic-variable",
      strLiteral -> "string", regex(numLiteral) -> "number", boolLiteral -> "boolean",
      op -> "operator")
      
    private def reduceFailures(failures: Set[Failure]) = {
      val expectedPowerSet = failures map { _.data } collect {
        case ExpectedLiteral(expect, _) =>
          Parsers.keySet filter { _.first contains expect.head } map Parsers
        
        case ExpectedRegex(regex) => {
          val first = {
            val back = RegexUtils first regex
            
            if (back contains None)
              UniversalCharSet
            else
              back flatMap { x => x }
          }
          
          Parsers.keySet filterNot { _.first intersect first isEmpty } map Parsers
        }
        
        case UnexpectedEndOfStream(Some(expect)) =>
          Parsers.keySet filter { _.first contains expect.head } map Parsers
      }
      
      val expectedCountPS = expectedPowerSet map { set => Map(set map { str => str -> 1 } toSeq: _*) }
      
      val expectedCounts = expectedCountPS.fold(Map()) { (left, right) =>
        right.foldLeft(left) {
          case (acc, (str, count)) =>
            acc.updated(str, acc get str map (count +) getOrElse count)
        }
      }
      
      val pairs = expectedCounts.toList.sortWith { case ((_, a), (_, b)) => a > b }
      
      val expectation = pairs.headOption flatMap {
        case (_, headCount) => {
          val (possibilities, _) = (pairs takeWhile { case (_, c) => headCount == c }).unzip
          
          if (possibilities.isEmpty)
            None
          else
            Some(possibilities mkString " or ")
        }
      }
      
      expectation map { ExpectedPattern format _ } getOrElse SyntaxPattern
    }
    
    // %%
    
    private lazy val op = (
        "where"
      | "with"
      | "union"
      | "intersect"
      | "difference"
      | "+"
      | "-"
      | "*"
      | "/"
      | "<"
      | "<="
      | ">"
      | ">="
      | "="
      | "!="
      | "&"
      | "|"
    )
  }
}
