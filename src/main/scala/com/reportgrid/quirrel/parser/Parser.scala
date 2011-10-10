package com.reportgrid.quirrel
package parser

import edu.uwm.cs.gll.RegexParsers

trait Parser extends RegexParsers with AST {
  
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
  )
  
  val formals: Parser[Vector[String]] = (
      formals ~ "," ~ ticId ^^ { (fs, _, f) => fs :+ f }
    | ticId                 ^^ { Vector(_) }
  )
  
  val actuals: Parser[Vector[Expr]] = (
      actuals ~ "," ~ expr ^^ { (es, _, e) => es :+ e }
    | expr                 ^^ { Vector(_) }
    | ""                   ^^^ Vector()
  )
  
  val properties: Parser[Vector[(String, Expr)]] = (
      properties ~ "," ~ property ^^ { (ps, _, p) => ps :+ p }
    | property                    ^^ { Vector(_) }
    | ""                          ^^^ Vector()
  )
  
  val property = propertyName ~ ":" ~ expr ^^ { (n, _, e) => (n, e) }
  
  val id = """[a-zA-Z_]['a-zA-Z_0-9]∗""".r
  
  val ticId = """'[a-zA-Z_0-9]['a-zA-Z_0-9]∗""".r
  
  val propertyName = """[a-zA-Z_][a-zA-Z_0-9]∗""".r
  
  val pathLiteral = """(//[a-zA-Z_\-0-9]+)+ """.r
  
  val strLiteral = """"([^"\n\r\\]|\\.)∗""""
  
  val numLiteral = """[0-9]+(.[0-9]+)?([eE][0-9]+)?""".r
  
  val boolLiteral = (
      "true"  ^^^ true
    | "false" ^^^ false
  )
  
  override val whitespace = """--.*|(-([^\-]|-[^)])∗-)|[;\s]+""".r
  override val skipWhitespace = true
}
