package com.reportgrid.quirrel
package parser

import edu.uwm.cs.gll.RegexParsers
import edu.uwm.cs.gll.ast.Filters

trait Parser extends RegexParsers with AST {
  
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
    | ""                   ^^^ Vector()
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
