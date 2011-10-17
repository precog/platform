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
  
  def parse(input: LineStream): Tree = {
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
      id ~ "(" ~ formals ~ ")" ~ ":=" ~ expr ~ expr ^^ { (id, _, fs, _, _, e1, e2) => Let(id, fs, e1, e2) }
    | id ~ ":=" ~ expr ~ expr                       ^^ { (id, _, e1, e2) => Let(id, Vector(), e1, e2) }
    
    | "new" ~ expr              ^^ { (_, e) => New(e) }
    | expr ~ "::" ~ expr ~ expr ^^ { (e1, _, e2, e3) => Relate(e1, e2, e3) }
    
    | id    ^^ { id => Dispatch(id, Vector()) }
    | ticId ^^ TicVar
    
    | pathLiteral ^^ StrLit
    | strLiteral  ^^ StrLit
    | numLiteral  ^^ NumLit
    | boolLiteral ^^ BoolLit
    
    | "{" ~ properties ~ "}"      ^^ { (_, ps, _) => ObjectDef(ps) }
    | "[" ~ nullableActuals ~ "]" ^^ { (_, as, _) => ArrayDef(as) }
    | expr ~ "." ~ propertyName   ^^ { (e, _, p) => Descent(e, p) }
    | expr ~ "[" ~ expr ~ "]"     ^^ { (e1, _, e2, _) => Deref(e1, e2) }
    
    | id ~ "(" ~ actuals ~ ")" ^^ { (id, _, as, _) => Dispatch(id, as) }
    | expr ~ opId ~ expr       ^^ Operation
    
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
    & ('relate <>)
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
