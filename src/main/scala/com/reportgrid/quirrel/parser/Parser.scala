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

import edu.uwm.cs.gll.Failure
import edu.uwm.cs.gll.LineStream
import edu.uwm.cs.gll.RegexParsers
import edu.uwm.cs.gll.Result
import edu.uwm.cs.gll.Success
import edu.uwm.cs.gll.ast.Filters

trait Parser extends RegexParsers with Filters with AST {
  
  def input: LineStream
  
  override lazy val root: Tree = {
    val results = expr(input)
    val successes = results collect { case Success(tree, _) => tree }
    
    if (successes.isEmpty)
      handleFailures(results)
    else
      handleSuccesses(successes)
  }
  
  def handleSuccesses(forest: Stream[Expr]): Tree = {
    if ((forest lengthCompare 1) > 0)
      throw new AssertionError("Fatal error: ambiguous parse results: " + forest.mkString(", "))
    else
      forest.head
  }
  
  def handleFailures(forest: Stream[Result[Expr]]): Nothing = {
    val failedForest = forest collect { case f: Failure => f }
    val sorted = failedForest.toList sort { _.tail.length < _.tail.length }
    val length = sorted.head.tail.length
    
    val failures = Set(sorted takeWhile { _.tail.length == length }: _*)
    throw ParseException(failures)
  }
  
  private[this] lazy val expr: Parser[Expr] = (
      id ~ "(" ~ formals ~ ")" ~ ":=" ~ expr ~ expr ^^ { (id, _, fs, _, _, e1, e2) => Binding(id, fs, e1, e2) }
    | id ~ ":=" ~ expr ~ expr                       ^^ { (id, _, e1, e2) => Binding(id, Vector(), e1, e2) }
    
    | "new" ~ expr              ^^ { (_, e) => New(e) }
    | expr ~ "::" ~ expr ~ expr ^^ { (e1, _, e2, e3) => Relate(e1, e2, e3) }
    
    | id    ^^ Var
    | ticId ^^ TicVar
    
    | pathLiteral ^^ StrLit
    | strLiteral  ^^ StrLit
    | numLiteral  ^^ NumLit
    | boolLiteral ^^ BoolLit
    
    | "{" ~ properties ~ "}"    ^^ { (_, ps, _) => ObjectDef(ps) }
    | "[" ~ actuals ~ "]"       ^^ { (_, as, _) => ArrayDef(as) }
    | expr ~ "." ~ propertyName ^^ { (e, _, p) => Descent(e, p) }
    | expr ~ "[" ~ expr ~ "]"   ^^ { (e1, _, e2, _) => Deref(e1, e2) }
    
    | id ~ "(" ~ actuals ~ ")" ^^ { (id, _, as, _) => Dispatch(id, as) }
    | expr ~ id ~ expr         ^^ Operation
    
    | expr ~ "+" ~ expr ^^ { (e1, _, e2) => Add(e1, e2) }
    | expr ~ "-" ~ expr ^^ { (e1, _, e2) => Sub(e1, e2) }
    | expr ~ "*" ~ expr ^^ { (e1, _, e2) => Mul(e1, e2) }
    | expr ~ "/" ~ expr ^^ { (e1, _, e2) => Div(e1, e2) }
    
    | expr ~ "<" ~ expr  ^^ { (e1, _, e2) => Lt(e1, e2) }
    | expr ~ "<=" ~ expr ^^ { (e1, _, e2) => LtEq(e1, e2) }
    | expr ~ ">" ~ expr  ^^ { (e1, _, e2) => Gt(e1, e2) }
    | expr ~ ">=" ~ expr ^^ { (e1, _, e2) => GtEq(e1, e2) }
    
    | expr ~ "=" ~ expr  ^^ { (e1, _, e2) => Eq(e1, e2) }
    | expr ~ "!=" ~ expr ^^ { (e1, _, e2) => NotEq(e1, e2) }
    
    | expr ~ "&" ~ expr ^^ { (e1, _, e2) => And(e1, e2) }
    | expr ~ "|" ~ expr ^^ { (e1, _, e2) => Or(e1, e2) }
    
    | "!" ~ expr ^^ { (_, e) => Comp(e) }
    | "~" ~ expr ^^ { (_, e) => Neg(e) }
    
    | "(" ~ expr ~ ")" ^^ { (_, e, _) => Paren(e) }
  ) filter (precedence & associativity)
  
  private[this] val formals: Parser[Vector[String]] = (
      formals ~ "," ~ ticId ^^ { (fs, _, f) => fs :+ f }
    | ticId                 ^^ { Vector(_) }
  )
  
  private[this] val actuals: Parser[Vector[Expr]] = (
      actuals ~ "," ~ expr ^^ { (es, _, e) => es :+ e }
    | expr                 ^^ { Vector(_) }
    | ""                   ^^^ Vector[Expr]()
  )
  
  private[this] val properties: Parser[Vector[(String, Expr)]] = (
      properties ~ "," ~ property ^^ { (ps, _, p) => ps :+ p }
    | property                    ^^ { Vector(_) }
    | ""                          ^^^ Vector()
  )
  
  private[this] val property = propertyName ~ ":" ~ expr ^^ { (n, _, e) => (n, e) }
  
  private[this] val id = """[a-zA-Z_]['a-zA-Z_0-9]∗""".r \ "new"
  
  private[this] val ticId = """'[a-zA-Z_0-9]['a-zA-Z_0-9]∗""".r
  
  private[this] val propertyName = """[a-zA-Z_][a-zA-Z_0-9]∗""".r
  
  private[this] val pathLiteral = """(//[a-zA-Z_\-0-9]+)+ """.r
  
  private[this] val strLiteral = """"([^"\n\r\\]|\\.)∗""""
  
  private[this] val numLiteral = """[0-9]+(.[0-9]+)?([eE][0-9]+)?""".r
  
  private[this] val boolLiteral = (
      "true"  ^^^ true
    | "false" ^^^ false
  )
  
  override val whitespace = """--.*|(-([^\-]|-[^)])∗-)|[;\s]+""".r
  override val skipWhitespace = true
  
  private[this] val precedence = 
    prec('dispatch, 'deref,
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
      'bind)
      
  private[this] val associativity = (
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
    & ('relate <>)
    & ('bind <)
  )
}
