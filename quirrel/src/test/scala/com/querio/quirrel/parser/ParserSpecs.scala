package com.querio.quirrel
package parser

import edu.uwm.cs.gll.LineStream
import org.scalacheck.Prop
import org.specs2.ScalaCheck
import org.specs2.mutable._

import java.io.File
import scala.io.Source

object ParserSpecs extends Specification with ScalaCheck with Parser with StubPhases {
  import ast._
  
  "uncomposed expression parsing" should {
    "accept parameterized bind with one parameter" in {
      parse("x('a) := 1 2") must beLike {
        case Let(_, "x", Vector("'a"), NumLit(_, "1"), NumLit(_, "2")) => ok
      }
    }
    
    "reject parameterized bind with no parameters" in {
      parse("x() := 1 2") must throwA[ParseException]
    }
    
    "reject parameterized bind with one missing expression" in {
      parse("x('a) := 1") must throwA[ParseException]
    }
    
    "reject parameterized bind with two missing expressions" in {
      parse("x('a) :=") must throwA[ParseException]
    }
    
    "accept parameterized bind with multiple parameter" in {
      parse("x('a, 'b, 'c) := 1 2") must beLike {
        case Let(_, "x", Vector("'a", "'b", "'c"), NumLit(_, "1"), NumLit(_, "2")) => ok
      }
    }
    
    "accept unparameterized bind" in {
      parse("x := 1 2") must beLike {
        case Let(_, "x", Vector(), NumLit(_, "1"), NumLit(_, "2")) => ok
      }
    }
    
    "reject unparameterized bind with one missing expression" in {
      parse("x := 1") must throwA[ParseException]
    }
    
    "reject unparameterized bind with two missing expressions" in {
      parse("x :=") must throwA[ParseException]
    }
    
    "accept a 'new' expression" in {
      parse("new 1") must beLike {
        case New(_, NumLit(_, "1")) => ok
      }
    }
    
    "accept a relate expression" in {
      parse("1 :: 2 3") must beLike {
        case Relate(_, NumLit(_, "1"), NumLit(_, "2"), NumLit(_, "3")) => ok
      }
    }
    
    "accept a relate expression with more than two constraint sets" in {
      parse("1 :: 2 :: 3 :: 4 5") must beLike {
        case Relate(_, NumLit(_, "1"), NumLit(_, "2"),
          Relate(_, NumLit(_, "2"), NumLit(_, "3"),
            Relate(_, NumLit(_, "3"), NumLit(_, "4"), NumLit(_, "5")))) => ok
      }
    }
    
    "accept a variable" in {
      parse("x") must beLike { case Dispatch(_, "x", Vector()) => ok }
      parse("cafe_Babe__42_") must beLike { case Dispatch(_, "cafe_Babe__42_", Vector()) => ok }
      parse("x'") must beLike { case Dispatch(_, "x'", Vector()) => ok }
    }
    
    "reject a variable named as a keyword" in {
      parse("new") must throwA[ParseException]
    }
    
    "reject a variable starting with a number" in {
      parse("2x") must throwA[ParseException]
      parse("123cafe_Babe__42_") must throwA[ParseException]
      parse("4x'") must throwA[ParseException]
    }
    
    "accept a tic-variable" in {
      parse("'x") must beLike { case TicVar(_, "'x") => ok }
      parse("'cafe_Babe__42_") must beLike { case TicVar(_, "'cafe_Babe__42_") => ok }
      parse("'x'") must beLike { case TicVar(_, "'x'") => ok }
    }
    
    "reject a tic-variable where the second character is a '" in {
      parse("''x") must throwA[ParseException]
      parse("''cafe_Babe__42_") must throwA[ParseException]
      parse("''x'") must throwA[ParseException]
    }
    
    "accept a path literal" in {
      parse("//foo") must beLike { case StrLit(_, "/foo") => ok }
      parse("//foo/bar/baz") must beLike { case StrLit(_, "/foo/bar/baz") => ok }
      parse("//cafe-babe42_silly/SILLY") must beLike { case StrLit(_, "/cafe-babe42_silly/SILLY") => ok }
    }
    
    "accept a string literal" in {
      parse("\"I have a dream\"") must beLike { case StrLit(_, "I have a dream") => ok }
      parse("\"\"") must beLike { case StrLit(_, "") => ok }
    }
    
    "reject a string literal that wraps onto a newline" in {
      parse("\"\n\"") must throwA[ParseException]
      parse("\"testing\n\"") must throwA[ParseException]
      parse("\"\none two three\"") must throwA[ParseException]
      parse("\"testing\none two three\"") must throwA[ParseException]
    }
    
    "reject an unterminated string literal" in {
      parse("\"") must throwA[ParseException]
      parse("\"testing") must throwA[ParseException]
    }
    
    "resolve all escape sequences in string literals" in {
      parse("\"\\\"\"") must beLike { case StrLit(_, "\"") => ok }
      parse("\"\\n\"") must beLike { case StrLit(_, "\n") => ok }
      parse("\"\\r\"") must beLike { case StrLit(_, "\r") => ok }
      parse("\"\\f\"") must beLike { case StrLit(_, "\f") => ok }
      parse("\"\\t\"") must beLike { case StrLit(_, "\t") => ok }
      parse("\"\\0\"") must beLike { case StrLit(_, "\0") => ok }
      parse("\"\\\\\"") must beLike { case StrLit(_, "\\") => ok }
    }
    
    "accept a number literal" in {
      parse("1") must beLike { case NumLit(_, "1") => ok }
      parse("256") must beLike { case NumLit(_, "256") => ok }
      parse("256.715") must beLike { case NumLit(_, "256.715") => ok }
      parse("3.1415") must beLike { case NumLit(_, "3.1415") => ok }
      parse("2.7183e26") must beLike { case NumLit(_, "2.7183e26") => ok }
      parse("2.7183E26") must beLike { case NumLit(_, "2.7183E26") => ok }
    }
    
    "reject a number literal ending with a ." in {
      parse("42.") must throwA[ParseException]
    }
    
    "accept a boolean literal" in {
      parse("true") must beLike { case BoolLit(_, true) => ok }
      parse("false") must beLike { case BoolLit(_, false) => ok }
    }
    
    "accept an object definition with no properties" in {
      parse("{}") must beLike { case ObjectDef(_, Vector()) => ok }
    }
    
    "reject an object definition with a spurious delimiter" in {
      parse("{,}") must throwA[ParseException]
    }
    
    "reject an unterminated object definition" in {
      parse("{") must throwA[ParseException]
      parse("{ a: 1") must throwA[ParseException]
      parse("{ a: 1,") must throwA[ParseException]
      parse("{ a: 1, b: 2, cafe: 3, star_BUckS: 4") must throwA[ParseException]
      parse("{ a: 1, b: 2, cafe: 3, star_BUckS: 4,") must throwA[ParseException]
    }
    
    "reject an object definition with expr content" in {
      parse("{ 42 }") must throwA[ParseException]
      parse("{ true }") must throwA[ParseException]
      parse("{ \"fourty-two\" }") must throwA[ParseException]
      parse("{{}}") must throwA[ParseException]
    }
    
    "accept an object definition with one property" in {
      parse("{ x: 1 }") must beLike { case ObjectDef(_, Vector(("x", NumLit(_, "1")))) => ok }
    }
    
    "reject an object definition with a trailing delimiter" in {
      parse("{ x: 1, }") must throwA[ParseException]
    }
    
    "accept an object definition with multiple properties" in {
      parse("{ a: 1, b: 2, cafe: 3, star_BUckS: 4 }") must beLike {
        case ObjectDef(_, Vector(("a", NumLit(_, "1")), ("b", NumLit(_, "2")), ("cafe", NumLit(_, "3")), ("star_BUckS", NumLit(_, "4")))) => ok
      }
    }
    
    "reject an object definition with undelimited properties" in {
      parse("{ a: 1, b: 2 cafe: 3, star_BUckS: 4 }") must throwA[ParseException]
      parse("{ a: 1 b: 2 cafe: 3 star_BUckS: 4 }") must throwA[ParseException]
    }
    
    "accept an array definition with no actuals" in {
      parse("[]") must beLike { case ArrayDef(_, Vector()) => ok }
    }
    
    "reject an array definition with a spurious delimiter" in {
      parse("[,]") must throwA[ParseException]
    }
    
    "reject an unterminated array definition" in {
      parse("[") must throwA[ParseException]
      parse("[1") must throwA[ParseException]
      parse("[1,") must throwA[ParseException]
      parse("[1, 2, 3, 4") must throwA[ParseException]
      parse("[1, 2, 3, 4,") must throwA[ParseException]
    }
    
    "accept an array definition with one actual" in {
      parse("[1]") must beLike { case ArrayDef(_, Vector(NumLit(_, "1"))) => ok }
    }
    
    "accept an array definition with multiple actuals" in {
      parse("[1, 2, 3]") must beLike {
        case ArrayDef(_, Vector(NumLit(_, "1"), NumLit(_, "2"), NumLit(_, "3"))) => ok
      }
    }
    
    "reject undelimited array definitions" in {
      parse("[1, 2 3]") must throwA[ParseException]
      parse("[1 2, 3]") must throwA[ParseException]
      parse("[1 2 3]") must throwA[ParseException]
    }
    
    "accept a property descent" in {
      parse("1.foo") must beLike { case Descent(_, NumLit(_, "1"), "foo") => ok }
      parse("1.e42") must beLike { case Descent(_, NumLit(_, "1"), "e42") => ok }
    }
    
    "reject property descent with invalid property" in {
      parse("1.-sdf") must throwA[ParseException]
      parse("1.42lkj") must throwA[ParseException]
    }
    
    "accept an array dereference" in {
      parse("1[2]") must beLike { case Deref(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
      parse("x[y]") must beLike { case Deref(_, Dispatch(_, "x", Vector()), Dispatch(_, "y", Vector())) => ok }
    }
    
    "reject an array dereference with multiple indexes" in {
      parse("1[1, 2]") must throwA[ParseException]
    }
    
    "reject an array dereference with no indexes" in {
      parse("1[]") must throwA[ParseException]
    }
    
    "accept a dispatch with one actual" in {
      parse("x(1)") must beLike { case Dispatch(_, "x", Vector(NumLit(_, "1"))) => ok }
    }
    
    "reject a dispatch with no actuals" in {
      parse("x()") must throwA[ParseException]
    }
    
    "reject a dispatch with undelimited actuals" in {
      parse("x(1, 2 3)") must throwA[ParseException]
      parse("x(1 2, 3)") must throwA[ParseException]
      parse("x(1 2 3)") must throwA[ParseException]
    }
    
    "reject a dispatch with one actual named as a keyword" in {
      parse("true(1)") must throwA[ParseException]
      parse("false(1)") must throwA[ParseException]
    }
    
    "accept a dispatch with multiple actuals" in {
      parse("x(1, 2, 3)") must beLike {
        case Dispatch(_, "x", Vector(NumLit(_, "1"), NumLit(_, "2"), NumLit(_, "3"))) => ok
      }
    }
    
    "reject a dispatch with multiple actuals named as a keyword" in {
      parse("new(1, 2, 3)") must throwA[ParseException]
      parse("true(1, 2, 3)") must throwA[ParseException]
      parse("false(1, 2, 3)") must throwA[ParseException]
    }
    
    "accept an infix operation" >> {
      "where" >> {
        parse("1 where 2") must beLike {
          case Operation(_, NumLit(_, "1"), "where", NumLit(_, "2")) => ok
        }
      }
      
      "with" >> {
        parse("1 with 2") must beLike {
          case Operation(_, NumLit(_, "1"), "with", NumLit(_, "2")) => ok
        }
      }
    }
    
    "reject an infix operation *not* named where" in {
      parse("1 x 2") must throwA[ParseException]
      parse("1 caFE_BABE42__ 2") must throwA[ParseException]
      parse("1 new 2") must throwA[ParseException]
      parse("1 true 2") must throwA[ParseException]
      parse("1 false 2") must throwA[ParseException]
    }
    
    "reject an infix operation lacking a left operand" in {
      parse("1 blah") must throwA[ParseException]
    }
    
    "reject an infix operation lacking a right operand" in {
      parse("blah 1") must throwA[ParseException]
    }
    
    "accept an addition operation" in {
      parse("1 + 2") must beLike { case Add(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject an addition operation lacking a left operand" in {
      parse("+ 2") must throwA[ParseException]
    }
    
    "reject an addition operation lacking a right operand" in {
      parse("1 +") must throwA[ParseException]
    }
    
    "accept a subtraction operation" in {
      parse("1 - 2") must beLike { case Sub(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a subtraction operation lacking a left operand" in {
      parse("- 2") must throwA[ParseException]
    }
    
    "reject a subtraction operation lacking a right operand" in {
      parse("1 -") must throwA[ParseException]
    }
    
    "accept a multiplication operation" in {
      parse("1 * 2") must beLike { case Mul(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a multiplication operation lacking a left operand" in {
      parse("* 2") must throwA[ParseException]
    }
    
    "reject a multiplication operation lacking a right operand" in {
      parse("1 *") must throwA[ParseException]
    }
    
    "accept a division operation" in {
      parse("1 / 2") must beLike { case Div(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a division operation lacking a left operand" in {
      parse("/ 2") must throwA[ParseException]
    }
    
    "reject a division operation lacking a right operand" in {
      parse("1 /") must throwA[ParseException]
    }
    
    "accept a less-than operation" in {
      parse("1 < 2") must beLike { case Lt(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a less-than operation lacking a left operand" in {
      parse("< 2") must throwA[ParseException]
    }
    
    "reject a less-than operation lacking a right operand" in {
      parse("1 <") must throwA[ParseException]
    }
    
    "accept a less-than-equal operation" in {
      parse("1 <= 2") must beLike { case LtEq(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a less-than-equal operation lacking a left operand" in {
      parse("<= 2") must throwA[ParseException]
    }
    
    "reject a less-than-equal operation lacking a right operand" in {
      parse("1 <=") must throwA[ParseException]
    }
    
    "accept a greater-than operation" in {
      parse("1 > 2") must beLike { case Gt(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a greater-than operation lacking a left operand" in {
      parse("> 2") must throwA[ParseException]
    }
    
    "reject a greater-than operation lacking a right operand" in {
      parse("1 >") must throwA[ParseException]
    }
    
    "accept a greater-than-equal operation" in {
      parse("1 >= 2") must beLike { case GtEq(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a greater-than-equal operation lacking a left operand" in {
      parse(">= 2") must throwA[ParseException]
    }
    
    "reject a greater-than-equal operation lacking a right operand" in {
      parse("1 >=") must throwA[ParseException]
    }
    
    "accept a equality operation" in {
      parse("1 = 2") must beLike { case Eq(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject an equality operation lacking a left operand" in {
      parse("= 2") must throwA[ParseException]
    }
    
    "reject an equality operation lacking a right operand" in {
      parse("1 =") must throwA[ParseException]
    }
    
    "accept a not-equal operation" in {
      parse("1 != 2") must beLike { case NotEq(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a not-equal operation lacking a left operand" in {
      parse("!= 2") must throwA[ParseException]
    }
    
    "reject a not-equal operation lacking a right operand" in {
      parse("1 !=") must throwA[ParseException]
    }
    
    "accept a boolean and operation" in {
      parse("1 & 2") must beLike { case And(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a boolean and operation lacking a left operand" in {
      parse("& 2") must throwA[ParseException]
    }
    
    "reject a boolean and operation lacking a right operand" in {
      parse("1 &") must throwA[ParseException]
    }
    
    "accept a boolean or operation" in {
      parse("1 | 2") must beLike { case Or(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a boolean or operation lacking a left operand" in {
      parse("| 2") must throwA[ParseException]
    }
    
    "reject a boolean or operation lacking a right operand" in {
      parse("1 |") must throwA[ParseException]
    }
    
    "accept boolean complementation" in {
      parse("!1") must beLike { case Comp(_, NumLit(_, "1")) => ok }
    }
    
    "accept numeric negation" in {
      parse("~1") must beLike { case Neg(_, NumLit(_, "1")) => ok }
    }
    
    "accept parentheticals" in {
      parse("(1)") must beLike { case Paren(_, NumLit(_, "1")) => ok }
    }
    
    "reject unmatched parentheses" in {
      parse("(") must throwA[ParseException]
      parse("(()") must throwA[ParseException]
    }
  }
  
  "operator precedence" should {
    "favor descent/deref over negation/complement" in {
      parse("!1.x") must beLike {
        case Comp(_, Descent(_, NumLit(_, "1"), "x")) => ok
      }
      
      parse("~1.x") must beLike {
        case Neg(_, Descent(_, NumLit(_, "1"), "x")) => ok
      }
      
      parse("!1[2]") must beLike {
        case Comp(_, Deref(_, NumLit(_, "1"), NumLit(_, "2"))) => ok
      }
      
      parse("~1[2]") must beLike {
        case Neg(_, Deref(_, NumLit(_, "1"), NumLit(_, "2"))) => ok
      }
    }
    
    "favor negation/complement over multiplication/division" in {
      parse("!a * b") must beLike { case Mul(_, Comp(_, Dispatch(_, "a", Vector())), Dispatch(_, "b", Vector())) => ok }
      parse("~a * b") must beLike { case Mul(_, Neg(_, Dispatch(_, "a", Vector())), Dispatch(_, "b", Vector())) => ok }
      parse("!a / b") must beLike { case Div(_, Comp(_, Dispatch(_, "a", Vector())), Dispatch(_, "b", Vector())) => ok }
      parse("~a / b") must beLike { case Div(_, Neg(_, Dispatch(_, "a", Vector())), Dispatch(_, "b", Vector())) => ok }
    }
    
    "favor multiplication/division over addition/subtraction" in {
      parse("a + b * c") must beLike { case Add(_, Dispatch(_, "a", Vector()), Mul(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a - b * c") must beLike { case Sub(_, Dispatch(_, "a", Vector()), Mul(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a * b + c") must beLike { case Add(_, Mul(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
      parse("a * b - c") must beLike { case Sub(_, Mul(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
      
      parse("a + b / c") must beLike { case Add(_, Dispatch(_, "a", Vector()), Div(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a - b / c") must beLike { case Sub(_, Dispatch(_, "a", Vector()), Div(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a / b + c") must beLike { case Add(_, Div(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
      parse("a / b - c") must beLike { case Sub(_, Div(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
    }
    
    "favor addition/subtraction over inequality operators" in {
      parse("a < b + c") must beLike { case Lt(_, Dispatch(_, "a", Vector()), Add(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a <= b + c") must beLike { case LtEq(_, Dispatch(_, "a", Vector()), Add(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a + b < c") must beLike { case Lt(_, Add(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
      parse("a + b <= c") must beLike { case LtEq(_, Add(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
      
      parse("a < b - c") must beLike { case Lt(_, Dispatch(_, "a", Vector()), Sub(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a <= b - c") must beLike { case LtEq(_, Dispatch(_, "a", Vector()), Sub(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a - b < c") must beLike { case Lt(_, Sub(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
      parse("a - b <= c") must beLike { case LtEq(_, Sub(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
                                                     
      parse("a > b + c") must beLike { case Gt(_, Dispatch(_, "a", Vector()), Add(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a >= b + c") must beLike { case GtEq(_, Dispatch(_, "a", Vector()), Add(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a + b > c") must beLike { case Gt(_, Add(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
      parse("a + b >= c") must beLike { case GtEq(_, Add(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
      
      parse("a > b - c") must beLike { case Gt(_, Dispatch(_, "a", Vector()), Sub(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a >= b - c") must beLike { case GtEq(_, Dispatch(_, "a", Vector()), Sub(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a - b > c") must beLike { case Gt(_, Sub(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
      parse("a - b >= c") must beLike { case GtEq(_, Sub(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
    }
    
    "favor inequality operators over equality operators" in {
      parse("a = b < c") must beLike { case Eq(_, Dispatch(_, "a", Vector()), Lt(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a != b < c") must beLike { case NotEq(_, Dispatch(_, "a", Vector()), Lt(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a < b = c") must beLike { case Eq(_, Lt(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
      parse("a < b != c") must beLike { case NotEq(_, Lt(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
      
      parse("a = b <= c") must beLike { case Eq(_, Dispatch(_, "a", Vector()), LtEq(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a != b <= c") must beLike { case NotEq(_, Dispatch(_, "a", Vector()), LtEq(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a <= b = c") must beLike { case Eq(_, LtEq(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
      parse("a <= b != c") must beLike { case NotEq(_, LtEq(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
      
      parse("a = b > c") must beLike { case Eq(_, Dispatch(_, "a", Vector()), Gt(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a != b > c") must beLike { case NotEq(_, Dispatch(_, "a", Vector()), Gt(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a > b = c") must beLike { case Eq(_, Gt(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
      parse("a > b != c") must beLike { case NotEq(_, Gt(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
      
      parse("a = b >= c") must beLike { case Eq(_, Dispatch(_, "a", Vector()), GtEq(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a != b >= c") must beLike { case NotEq(_, Dispatch(_, "a", Vector()), GtEq(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a >= b = c") must beLike { case Eq(_, GtEq(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
      parse("a >= b != c") must beLike { case NotEq(_, GtEq(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
    }
    
    "favor equality operators over and/or" in {
      parse("a & b = c") must beLike { case And(_, Dispatch(_, "a", Vector()), Eq(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a | b = c") must beLike { case Or(_, Dispatch(_, "a", Vector()), Eq(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a = b & c") must beLike { case And(_, Eq(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
      parse("a = b | c") must beLike { case Or(_, Eq(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
      
      parse("a & b != c") must beLike { case And(_, Dispatch(_, "a", Vector()), NotEq(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a | b != c") must beLike { case Or(_, Dispatch(_, "a", Vector()), NotEq(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()))) => ok }
      parse("a != b & c") must beLike { case And(_, NotEq(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
      parse("a != b | c") must beLike { case Or(_, NotEq(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
    }
    
    "favor and/or operators over new" in {
      parse("new a & b") must beLike { case New(_, And(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector()))) => ok }
      parse("new a | b") must beLike { case New(_, Or(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector()))) => ok }
    }
    
    "favor new over where" in {
      parse("new a where b") must beLike { case Operation(_, New(_, Dispatch(_, "a", Vector())), "where", Dispatch(_, "b", Vector())) => ok }
    }
    
    "favor where over relate" in {
      parse("a where b :: c d") must beLike { case Relate(_, Operation(_, Dispatch(_, "a", Vector()), "where", Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector()), Dispatch(_, "d", Vector())) => ok }
      parse("a :: b where c d") must beLike { case Relate(_, Dispatch(_, "a", Vector()), Operation(_, Dispatch(_, "b", Vector()), "where", Dispatch(_, "c", Vector())), Dispatch(_, "d", Vector())) => ok }
    }
  }
  
  "operator associativity" should {
    "associate relations to the right" in {
      parse("a :: b a :: b 42") must beLike {
        case Relate(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector()),
               Relate(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector()),
                 NumLit(_, "42"))) => ok
      }
    }
    
    "associate multiplication to the left" in {
      parse("a * b * c") must beLike { case Mul(_, Mul(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
    }
    
    "associate division to the left" in {
      parse("a / b / c") must beLike { case Div(_, Div(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
    }
    
    "associate addition to the left" in {
      parse("a + b + c") must beLike { case Add(_, Add(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
    }
    
    "associate subtraction to the left" in {
      parse("a - b - c") must beLike { case Sub(_, Sub(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
    }
    
    "associate less-than to the left" in {
      parse("a < b < c") must beLike { case Lt(_, Lt(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
    }
    
    "associate less-than-equal to the left" in {
      parse("a <= b <= c") must beLike { case LtEq(_, LtEq(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
    }
    
    "associate greater-than to the left" in {
      parse("a > b > c") must beLike { case Gt(_, Gt(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
    }
    
    "associate greater-than-equal to the left" in {
      parse("a >= b >= c") must beLike { case GtEq(_, GtEq(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
    }
    
    "associate equal to the left" in {
      parse("a = b = c") must beLike { case Eq(_, Eq(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
    }
    
    "associate not-equal to the left" in {
      parse("a != b != c") must beLike { case NotEq(_, NotEq(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector())), Dispatch(_, "c", Vector())) => ok }
    }
    
    "associate where to the left" in {
      parse("a where b where c") must beLike { case Operation(_, Operation(_, Dispatch(_, "a", Vector()), "where", Dispatch(_, "b", Vector())), "where", Dispatch(_, "c", Vector())) => ok }
    }
  }
  
  "whitespace processing" should {
    "skip any amount of leading space characters" in {
      parse(" 1") must beLike { case NumLit(_, "1") => ok }
      parse("     1") must beLike { case NumLit(_, "1") => ok }
      parse("\t  \t 1") must beLike { case NumLit(_, "1") => ok }
      parse("\n\r  ;\r\t  \t\n ;;1") must beLike { case NumLit(_, "1") => ok }
    }
    
    "skip any amount of trailing space characters" in {
      parse("1 ") must beLike { case NumLit(_, "1") => ok }
      parse("1     ") must beLike { case NumLit(_, "1") => ok }
      parse("1\t  \t ") must beLike { case NumLit(_, "1") => ok }
      parse("1\n\r  ;\r\t  \t\n ;;") must beLike { case NumLit(_, "1") => ok }
    }
    
    "skip leading line comments delimited by newline" in {
      parse("-- testing one two three\n1") must beLike { case NumLit(_, "1") => ok }
      parse("-- testing one two three\n--four five six\n1") must beLike { case NumLit(_, "1") => ok }
      parse("   \t  -- testing one two three\n\n  \t--four five six\n\r  1") must beLike {
        case NumLit(_, "1") => ok
      }
    }
    
    "skip trailing line comments delimited by newline" in {
      parse("1-- testing one two three\n") must beLike { case NumLit(_, "1") => ok }
      parse("1-- testing one two three\n--four five six\n") must beLike { case NumLit(_, "1") => ok }
      parse("1   \t  -- testing one two three\n\n  \t--four five six\n\r  ") must beLike {
        case NumLit(_, "1") => ok
      }
    }
    
    "skip leading block comments" in {
      parse("(- testing one two three -)1") must beLike { case NumLit(_, "1") => ok }
      parse("  (- testing one two three -)   1") must beLike { case NumLit(_, "1") => ok }
      parse("  (- testing one \n two\n three -)   1") must beLike { case NumLit(_, "1") => ok }
      
      parse("(- testing one two three -)\n(-four five six -)\n1") must beLike {
        case NumLit(_, "1") => ok
      }
      
      parse("(- testing one- \\- two three -)\n(-four \n five-- \t six -)\n1") must beLike {
        case NumLit(_, "1") => ok
      }
      
      parse("   \t  (- testing one two three -)\n\n  \t(- four five six -)\n\r  1") must beLike {
        case NumLit(_, "1") => ok
      }
    }
    
    "skip trailing line comments delimited by newline" in {
      parse("1(- testing one two three -)") must beLike { case NumLit(_, "1") => ok }
      parse("1  (- testing one two three -)    ") must beLike { case NumLit(_, "1") => ok }
      parse("1  (- testing one \n two\n three -)  ") must beLike { case NumLit(_, "1") => ok }
      
      parse("1\t  (- testing one two three -)\n(-four five six -)\n") must beLike {
        case NumLit(_, "1") => ok
      }
      
      parse("1(- testing one- \\- two three -)\n(-four \n five-- \t six -)\n") must beLike {
        case NumLit(_, "1") => ok
      }
      
      parse("1   \t  (- testing one two three -)\n\n  \t(- four five six -)\n\r  ") must beLike {
        case NumLit(_, "1") => ok
      }
    }
  }
  
  "composed expression parsing" should {
    "parse a no param function containing a parenthetical" in {
      parse("a := 1 (2)") must beLike {
        case Let(_, "a", Vector(), NumLit(_, "1"), Paren(_, NumLit(_, "2"))) => ok
      }
    }
    
    "parse a no param function containing a no param function" in {
      parse("a := 1 c := 2 3") must beLike {
        case Let(_, "a", Vector(), NumLit(_, "1"), Let(_, "c", Vector(), NumLit(_, "2"), NumLit(_, "3"))) => ok
      }
    }
    
    "parse a no param function containing a 1 param function" in {
      parse("a := 1 c('d) := 2 3") must beLike {
        case Let(_, "a", Vector(), NumLit(_, "1"), Let(_, "c", Vector("'d"), NumLit(_, "2"), NumLit(_, "3"))) => ok
      }
    }
    
    "correctly nest multiple binds" in {
      val input = """
        | a :=
        |   b := dataset(//f)
        |   c := dataset(//g)
        |
        |   d
        | e""".stripMargin
      
      parse(input) must beLike {
        case Let(_, "a", Vector(), Let(_, "b", Vector(), Dispatch(_, "dataset", Vector(StrLit(_, "/f"))), Let(_, "c", Vector(), Dispatch(_, "dataset", Vector(StrLit(_, "/g"))), Dispatch(_, "d", Vector()))), Dispatch(_, "e", Vector())) => ok
      }
    }
    
    "handle new expression within deref parameter" in {
      parse("1[new 2]") must beLike {
        case Deref(_, NumLit(_, "1"), New(_, NumLit(_, "2"))) => ok
      }
    }
    
    "accept a binary expression involving a number and a string" in {
      parse("1 + \"a\"") must beLike {
        case Add(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parse("1 - \"a\"") must beLike {
        case Sub(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parse("1 * \"a\"") must beLike {
        case Mul(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parse("1 / \"a\"") must beLike {
        case Div(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parse("1 < \"a\"") must beLike {
        case Lt(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parse("1 <= \"a\"") must beLike {
        case LtEq(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parse("1 > \"a\"") must beLike {
        case Gt(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parse("1 >= \"a\"") must beLike {
        case GtEq(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parse("1 = \"a\"") must beLike {
        case Eq(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parse("1 != \"a\"") must beLike {
        case NotEq(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parse("1 & \"a\"") must beLike {
        case And(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parse("1 | \"a\"") must beLike {
        case Or(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parse("1 where \"a\"") must beLike {
        case Operation(_, NumLit(_, "1"), "where", StrLit(_, "a")) => ok
      }
    }
  }
  
  val exampleDir = new File("quirrel/examples")
  
  if (exampleDir.exists) {
    "specification examples" >> {
      for (file <- exampleDir.listFiles if file.getName endsWith ".qrl") {
        file.getName >> {
          parse(LineStream(Source.fromFile(file))) must not(throwA[Throwable])
        }
      }
    }
  } else {
    "specification examples" >> skipped
  }
  
  "global ambiguity resolution" should {
    "associate paired consecutive parentheses" in {
      parse("a := b := c (d) (e)") must beLike {
        case Let(_, "a", Vector(), Let(_, "b", Vector(), Dispatch(_, "c", Vector()), Paren(_, Dispatch(_, "d", Vector()))), Paren(_, Dispatch(_, "e", Vector()))) => ok
      }
    }
    
    "disambiguate one-argument function within n-ary relation" in {
      val input = """
        | a ::
        |   b :: c
        |     d := f
        |     (1)
        |   2""".stripMargin
        
      parse(input) must beLike {
        case Relate(_, Dispatch(_, "a", Vector()), Dispatch(_, "b", Vector()),
          Relate(_, Dispatch(_, "b", Vector()), Dispatch(_, "c", Vector()),
            Let(_, "d", Vector(), Dispatch(_, "f", Vector(NumLit(_, "1"))), NumLit(_, "2")))) => ok
      }
    }
  }
}
