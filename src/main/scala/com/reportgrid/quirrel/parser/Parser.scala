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
package com.reportgrid.quirrel
package parser

import edu.uwm.cs.gll.ExpectedLiteral
import edu.uwm.cs.gll.ExpectedRegex
import edu.uwm.cs.gll.Failure
import edu.uwm.cs.gll.LineStream
import edu.uwm.cs.gll.RegexParsers
import edu.uwm.cs.gll.RegexUtils
import edu.uwm.cs.gll.Result
import edu.uwm.cs.gll.Success
import edu.uwm.cs.gll.UnexpectedEndOfStream
import edu.uwm.cs.gll.ast.Filters
import edu.uwm.cs.util.ComplementarySet
import edu.uwm.cs.util.UniversalCharSet

trait Parser extends RegexParsers with Filters with AST {
  
  def parse(input: LineStream) = {
    val results = expr(input)
    val successes = results collect { case Success(tree, _) => tree }
    val failures = results collect { case f: Failure => f }
    
    if (successes.isEmpty)
      handleFailures(failures)
    else
      handleSuccesses(successes)
  }
  
  def handleSuccesses(forest: Stream[Expr]): Expr = {
    val root = if ((forest lengthCompare 1) > 0)
      throw new AssertionError("Fatal error: ambiguous parse results: " + forest.mkString(", "))
    else
      forest.head
    
    def bindRoot(e: Expr) {
      e._root() = root
      
      e.productIterator foreach {
        case e: Expr => bindRoot(e)
        case v: Vector[_] => v foreach bindElements
        case _ =>
      }
    }
    
    def bindElements(a: Any) {
      a match {
        case e: Expr => bindRoot(e)
        
        case (e1: Expr, e2: Expr) => {
          bindRoot(e1)
          bindRoot(e2)
        }
        
        case (e: Expr, _) => bindRoot(e)
        case (_, e: Expr) => bindRoot(e)
        
        case _ =>
      }
    }
    
    bindRoot(root)
    root
  }
  
  def handleFailures(forest: Stream[Failure]): Nothing = {
    val sorted = forest.toList sort { _.tail.length < _.tail.length }
    val length = sorted.head.tail.length
    
    val failures = Set(sorted takeWhile { _.tail.length == length }: _*)
    throw ParseException(failures)
  }
  
  // %%
  
  lazy val expr: Parser[Expr] = (
      id ~ "(" ~ formals ~ ")" ~ ":=" ~ expr ~ expr ^# { (loc, id, _, fs, _, _, e1, e2) => Let(loc, id, fs, e1, e2) }
    | id ~ ":=" ~ expr ~ expr                       ^# { (loc, id, _, e1, e2) => Let(loc, id, Vector(), e1, e2) }
    
    | "new" ~ expr              ^# { (loc, _, e) => New(loc, e) }
    | expr ~ "::" ~ expr ~ expr ^# { (loc, e1, _, e2, e3) => Relate(loc, e1, e2, e3) }
    
    | id    ^# { (loc, id) => Dispatch(loc, id, Vector()) }
    | ticId ^# TicVar
    
    | pathLiteral ^# StrLit
    | strLiteral  ^# StrLit
    | numLiteral  ^# NumLit
    | boolLiteral ^# BoolLit
    
    | "{" ~ properties ~ "}"      ^# { (loc, _, ps, _) => ObjectDef(loc, ps) }
    | "[" ~ nullableActuals ~ "]" ^# { (loc, _, as, _) => ArrayDef(loc, as) }
    | expr ~ "." ~ propertyName   ^# { (loc, e, _, p) => Descent(loc, e, p) }
    | expr ~ "[" ~ expr ~ "]"     ^# { (loc, e1, _, e2, _) => Deref(loc, e1, e2) }
    
    | id ~ "(" ~ actuals ~ ")" ^# { (loc, id, _, as, _) => Dispatch(loc, id, as) }
    | expr ~ opId ~ expr       ^# Operation
    
    | expr ~ "+" ~ expr ^# { (loc, e1, _, e2) => Add(loc, e1, e2) }
    | expr ~ "-" ~ expr ^# { (loc, e1, _, e2) => Sub(loc, e1, e2) }
    | expr ~ "*" ~ expr ^# { (loc, e1, _, e2) => Mul(loc, e1, e2) }
    | expr ~ "/" ~ expr ^# { (loc, e1, _, e2) => Div(loc, e1, e2) }
    
    | expr ~ "<" ~ expr  ^# { (loc, e1, _, e2) => Lt(loc, e1, e2) }
    | expr ~ "<=" ~ expr ^# { (loc, e1, _, e2) => LtEq(loc, e1, e2) }
    | expr ~ ">" ~ expr  ^# { (loc, e1, _, e2) => Gt(loc, e1, e2) }
    | expr ~ ">=" ~ expr ^# { (loc, e1, _, e2) => GtEq(loc, e1, e2) }
    
    | expr ~ "=" ~ expr  ^# { (loc, e1, _, e2) => Eq(loc, e1, e2) }
    | expr ~ "!=" ~ expr ^# { (loc, e1, _, e2) => NotEq(loc, e1, e2) }
    
    | expr ~ "&" ~ expr ^# { (loc, e1, _, e2) => And(loc, e1, e2) }
    | expr ~ "|" ~ expr ^# { (loc, e1, _, e2) => Or(loc, e1, e2) }
    
    | "!" ~ expr ^# { (loc, _, e) => Comp(loc, e) }
    | "~" ~ expr ^# { (loc, _, e) => Neg(loc, e) }
    
    | "(" ~ expr ~ ")" ^# { (loc, _, e, _) => Paren(loc, e) }
  ) filter (precedence & associativity)
  
  lazy val formals: Parser[Vector[String]] = (
      formals ~ "," ~ ticId ^^ { (fs, _, f) => fs :+ f }
    | ticId                 ^^ { Vector(_) }
  )
  
  lazy val actuals: Parser[Vector[Expr]] = (
      actuals ~ "," ~ expr ^^ { (es, _, e) => es :+ e }
    | expr                 ^^ { Vector(_) }
  )
  
  lazy val nullableActuals = (
      actuals
    | ""      ^^^ Vector[Expr]()
  )
  
  lazy val properties: Parser[Vector[(String, Expr)]] = (
      properties ~ "," ~ property ^^ { (ps, _, p) => ps :+ p }
    | property                    ^^ { Vector(_) }
    | ""                          ^^^ Vector()
  )
  
  lazy val property = propertyName ~ ":" ~ expr ^^ { (n, _, e) => (n, e) }
  
  lazy val id = """[a-zA-Z_]['a-zA-Z_0-9]*""".r \ keywords
  
  val opId = "where"
  
  val ticId = """'[a-zA-Z_0-9]['a-zA-Z_0-9]*""".r
  
  val propertyName = """[a-zA-Z_][a-zA-Z_0-9]*""".r
  
  val pathLiteral = """/(/[a-zA-Z_\-0-9]+)+""".r ^^ canonicalizePath
  
  val strLiteral = """"([^\n\r\\]|\\.)*"""".r ^^ canonicalizeStr
  
  val numLiteral = """[0-9]+(\.[0-9]+)?([eE][0-9]+)?""".r
  
  val boolLiteral: Parser[Boolean] = (
      "true"  ^^^ true
    | "false" ^^^ false
  )
  
  val keywords = "new|true|false|where".r
  
  val operations = "where".r
  
  override val whitespace = """([;\s]+|--.*|\(-([^\-]|-[^)])*-\))+""".r
  override val skipWhitespace = true
  
  val precedence = 
    prec('descent, 'deref,
      'comp, 'neg,
      'mul, 'div,
      'add, 'sub,
      'lt, 'lteq, 'gt, 'gteq,
      'eq, 'noteq,
      'and, 'or,
      'op,
      'new,
      'where,
      'relate,
      'let)
      
  val associativity = (
      ('mul <)
    & ('div <)
    & ('add <)
    & ('sub <)
    & ('lt <)
    & ('lteq <)
    & ('gt <)
    & ('gteq <)
    & ('eq <)
    & ('noteq <)
    & ('and <)
    & ('or <)
    & ('op <)
    & ('where <)
    & ('relate >)
  )
  
  // %%
  
  def canonicalizeStr(str: String): String = {
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
  
  def canonicalizePath(str: String): String = str substring 1
  
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
      
    def reduceFailures(failures: Set[Failure]) = {
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
          val (possibilities, _) = List unzip (pairs takeWhile { case (_, c) => headCount == c })
          val biased = if ((possibilities lengthCompare 1) > 0)
            possibilities filter ("expression" !=)
          else
            possibilities
          
          if (biased.isEmpty)
            None
          else
            Some(biased mkString " or ")
        }
      }
      
      expectation map { ExpectedPattern format _ } getOrElse SyntaxPattern
    }
    
    // %%
    
    private lazy val op = (
        opId
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
