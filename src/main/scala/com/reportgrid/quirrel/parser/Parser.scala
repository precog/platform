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
      
      e match {
        case Binding(_, _, left, right) => {
          bindRoot(left)
          bindRoot(right)
        }
        
        case New(child) => bindRoot(child)
        
        case Relate(from, to, in) => {
          bindRoot(from)
          bindRoot(to)
          bindRoot(in)
        }
        
        case Var(_) =>
        case TicVar(_) =>
        case StrLit(_) =>
        case NumLit(_) =>
        case BoolLit(_) =>
        
        case ObjectDef(props) => {
          for ((_, e) <- props) {
            bindRoot(e)
          }
        }
        
        case ArrayDef(values) => values foreach bindRoot
        
        case Descent(child, _) => bindRoot(child)
        
        case Deref(left, right) => {
          bindRoot(left)
          bindRoot(right)
        }
        
        case Dispatch(_, actuals) => actuals foreach bindRoot
        
        case Operation(left, _, right) => {
          bindRoot(left)
          bindRoot(right)
        }
        
        case Add(left, right) => {
          bindRoot(left)
          bindRoot(right)
        }
        
        case Sub(left, right) => {
          bindRoot(left)
          bindRoot(right)
        }
        
        case Mul(left, right) => {
          bindRoot(left)
          bindRoot(right)
        }
        
        case Div(left, right) => {
          bindRoot(left)
          bindRoot(right)
        }
        
        case Lt(left, right) => {
          bindRoot(left)
          bindRoot(right)
        }
        
        case LtEq(left, right) => {
          bindRoot(left)
          bindRoot(right)
        }
        
        case Gt(left, right) => {
          bindRoot(left)
          bindRoot(right)
        }
        
        case GtEq(left, right) => {
          bindRoot(left)
          bindRoot(right)
        }
        
        case Eq(left, right) => {
          bindRoot(left)
          bindRoot(right)
        }
        
        case NotEq(left, right) => {
          bindRoot(left)
          bindRoot(right)
        }
        
        case And(left, right) => {
          bindRoot(left)
          bindRoot(right)
        }
        
        case Or(left, right) => {
          bindRoot(left)
          bindRoot(right)
        }
        
        case Comp(child) => bindRoot(child)
        
        case Neg(child) => bindRoot(child)
        
        case Paren(child) => bindRoot(child)
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
  
  lazy val formals: Parser[Vector[String]] = (
      formals ~ "," ~ ticId ^^ { (fs, _, f) => fs :+ f }
    | ticId                 ^^ { Vector(_) }
  )
  
  lazy val actuals: Parser[Vector[Expr]] = (
      actuals ~ "," ~ expr ^^ { (es, _, e) => es :+ e }
    | expr                 ^^ { Vector(_) }
    | ""                   ^^^ Vector[Expr]()
  )
  
  lazy val properties: Parser[Vector[(String, Expr)]] = (
      properties ~ "," ~ property ^^ { (ps, _, p) => ps :+ p }
    | property                    ^^ { Vector(_) }
    | ""                          ^^^ Vector()
  )
  
  lazy val property = propertyName ~ ":" ~ expr ^^ { (n, _, e) => (n, e) }
  
  lazy val id = """[a-zA-Z_]['a-zA-Z_0-9]*""".r \ keywords
  
  val ticId = """'[a-zA-Z_0-9]['a-zA-Z_0-9]*""".r
  
  val propertyName = """[a-zA-Z_][a-zA-Z_0-9]*""".r
  
  val pathLiteral = """(//[a-zA-Z_\-0-9]+)+""".r
  
  val strLiteral = """"([^\n\r\\]|\\.)*"""".r
  
  val numLiteral = """[0-9]+(\.[0-9]+)?([eE][0-9]+)?""".r
  
  val boolLiteral: Parser[Boolean] = (
      "true"  ^^^ true
    | "false" ^^^ false
  )
  
  val keywords = "new|true|false".r
  
  override val whitespace = """[;\s]+|[;\s]*(--.*[;\s]*)+|[;\s]*(\(-([^\-]|-[^)])*-\)[;\s]*)+""".r
  override val skipWhitespace = true
  
  val precedence = 
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
    & ('bind <)
  )
  
  // %%
  
  case class ParseException(failures: Set[Failure]) extends RuntimeException {
    def mkString = {
      val tail = failures.head.tail      // if we have an empty set of failures, that's bad
      tail formatError (ParseException reduceFailures failures)
    }
  }
  
  object ParseException extends (Set[Failure] => ParseException) {
    private val ExpectedPattern = "error:%%d: expected %s%n  %%s%n  %%s"
    private val SyntaxPattern = "error:%d: syntax error%n  %s%n  %s"
    
    private lazy val Parsers = Map(expr -> "expression", property -> "property",
      regex(pathLiteral) -> "path", id -> "identifier", regex(ticId) -> "tic-variable",
      regex(strLiteral) -> "string", regex(numLiteral) -> "number", boolLiteral -> "boolean",
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
        id
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
