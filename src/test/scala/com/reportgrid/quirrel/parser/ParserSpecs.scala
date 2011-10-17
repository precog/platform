package com.reportgrid.quirrel
package parser

import edu.uwm.cs.gll.LineStream
import org.scalacheck.Prop
import org.specs2.ScalaCheck
import org.specs2.mutable._

object ParserSpecs extends Specification with ScalaCheck with Parser {
  
  "uncomposed expression parsing" should {
    "accept parameterized bind with one parameter" in {
      parse("x('a) := 1 2") mustEqual Binding("x", Vector("'a"), NumLit("1"), NumLit("2"))
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
      parse("x('a, 'b, 'c) := 1 2") mustEqual Binding("x", Vector("'a", "'b", "'c"), NumLit("1"), NumLit("2"))
    }
    
    "accept unparameterized bind" in {
      parse("x := 1 2") mustEqual Binding("x", Vector(), NumLit("1"), NumLit("2"))
    }
    
    "reject unparameterized bind with one missing expression" in {
      parse("x := 1") must throwA[ParseException]
    }
    
    "reject unparameterized bind with two missing expressions" in {
      parse("x :=") must throwA[ParseException]
    }
    
    "accept a 'new' expression" in {
      parse("new 1") mustEqual New(NumLit("1"))
    }
    
    "accept a relate expression" in {
      parse("1 :: 2 3") mustEqual Relate(NumLit("1"), NumLit("2"), NumLit("3"))
    }
    
    "accept a variable" in {
      parse("x") mustEqual Var("x")
      parse("cafe_Babe__42_") mustEqual Var("cafe_Babe__42_")
      parse("x'") mustEqual Var("x'")
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
      parse("'x") mustEqual TicVar("'x")
      parse("'cafe_Babe__42_") mustEqual TicVar("'cafe_Babe__42_")
      parse("'x'") mustEqual TicVar("'x'")
    }
    
    "reject a tic-variable where the second character is a '" in {
      parse("''x") must throwA[ParseException]
      parse("''cafe_Babe__42_") must throwA[ParseException]
      parse("''x'") must throwA[ParseException]
    }
    
    "accept a path literal" in {
      parse("//foo") mustEqual StrLit("/foo")
      parse("//foo/bar/baz") mustEqual StrLit("/foo/bar/baz")
      parse("//cafe-babe42_silly/SILLY") mustEqual StrLit("/cafe-babe42_silly/SILLY")
    }
    
    "accept a string literal" in {
      parse("\"I have a dream\"") mustEqual StrLit("I have a dream")
      parse("\"\"") mustEqual StrLit("")
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
      parse("\"\\\"\"") mustEqual StrLit("\"")
      parse("\"\\n\"") mustEqual StrLit("\n")
      parse("\"\\r\"") mustEqual StrLit("\r")
      parse("\"\\f\"") mustEqual StrLit("\f")
      parse("\"\\t\"") mustEqual StrLit("\t")
      parse("\"\\0\"") mustEqual StrLit("\0")
      parse("\"\\\\\"") mustEqual StrLit("\\")
    }
    
    "accept a number literal" in {
      parse("1") mustEqual NumLit("1")
      parse("256") mustEqual NumLit("256")
      parse("256.715") mustEqual NumLit("256.715")
      parse("3.1415") mustEqual NumLit("3.1415")
      parse("2.7183e26") mustEqual NumLit("2.7183e26")
      parse("2.7183E26") mustEqual NumLit("2.7183E26")
    }
    
    "reject a number literal ending with a ." in {
      parse("42.") must throwA[ParseException]
    }
    
    "accept a boolean literal" in {
      parse("true") mustEqual BoolLit(true)
      parse("false") mustEqual BoolLit(false)
    }
    
    "accept an object definition with no properties" in {
      parse("{}") mustEqual ObjectDef(Vector())
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
      parse("{ x: 1 }") mustEqual ObjectDef(Vector("x" -> NumLit("1")))
    }
    
    "reject an object definition with a trailing delimiter" in {
      parse("{ x: 1, }") must throwA[ParseException]
    }
    
    "accept an object definition with multiple properties" in {
      val expected =  ObjectDef(Vector("a" -> NumLit("1"), "b" -> NumLit("2"), "cafe" -> NumLit("3"), "star_BUckS" -> NumLit("4")))
      parse("{ a: 1, b: 2, cafe: 3, star_BUckS: 4 }") mustEqual expected
    }
    
    "reject an object definition with undelimited properties" in {
      parse("{ a: 1, b: 2 cafe: 3, star_BUckS: 4 }") must throwA[ParseException]
      parse("{ a: 1 b: 2 cafe: 3 star_BUckS: 4 }") must throwA[ParseException]
    }
    
    "accept an array definition with no actuals" in {
      parse("[]") mustEqual ArrayDef(Vector())
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
      parse("[1]") mustEqual ArrayDef(Vector(NumLit("1")))
    }
    
    "accept an array definition with multiple actuals" in {
      parse("[1, 2, 3]") mustEqual ArrayDef(Vector(NumLit("1"), NumLit("2"), NumLit("3")))
    }
    
    "reject undelimited array definitions" in {
      parse("[1, 2 3]") must throwA[ParseException]
      parse("[1 2, 3]") must throwA[ParseException]
      parse("[1 2 3]") must throwA[ParseException]
    }
    
    "accept a property descent" in {
      parse("1.foo") mustEqual Descent(NumLit("1"), "foo")
      parse("1.e42") mustEqual Descent(NumLit("1"), "e42")
    }
    
    "reject property descent with invalid property" in {
      parse("1.-sdf") must throwA[ParseException]
      parse("1.42lkj") must throwA[ParseException]
    }
    
    "accept an array dereference" in {
      parse("1[2]") mustEqual Deref(NumLit("1"), NumLit("2"))
      parse("x[y]") mustEqual Deref(Var("x"), Var("y"))
    }
    
    "reject an array dereference with multiple indexes" in {
      parse("1[1, 2]") must throwA[ParseException]
    }
    
    "reject an array dereference with no indexes" in {
      parse("1[]") must throwA[ParseException]
    }
    
    "accept a dispatch with one actual" in {
      parse("x(1)") mustEqual Dispatch("x", Vector(NumLit("1")))
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
      parse("x(1, 2, 3)") mustEqual Dispatch("x", Vector(NumLit("1"), NumLit("2"), NumLit("3")))
    }
    
    "reject a dispatch with multiple actuals named as a keyword" in {
      parse("new(1, 2, 3)") must throwA[ParseException]
      parse("true(1, 2, 3)") must throwA[ParseException]
      parse("false(1, 2, 3)") must throwA[ParseException]
    }
    
    "accept an infix operation" in {
      parse("1 where 2") mustEqual Operation(NumLit("1"), "where", NumLit("2"))
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
      parse("1 + 2") mustEqual Add(NumLit("1"), NumLit("2"))
    }
    
    "reject an addition operation lacking a left operand" in {
      parse("+ 2") must throwA[ParseException]
    }
    
    "reject an addition operation lacking a right operand" in {
      parse("1 +") must throwA[ParseException]
    }
    
    "accept a subtraction operation" in {
      parse("1 - 2") mustEqual Sub(NumLit("1"), NumLit("2"))
    }
    
    "reject a subtraction operation lacking a left operand" in {
      parse("- 2") must throwA[ParseException]
    }
    
    "reject a subtraction operation lacking a right operand" in {
      parse("1 -") must throwA[ParseException]
    }
    
    "accept a multiplication operation" in {
      parse("1 * 2") mustEqual Mul(NumLit("1"), NumLit("2"))
    }
    
    "reject a multiplication operation lacking a left operand" in {
      parse("* 2") must throwA[ParseException]
    }
    
    "reject a multiplication operation lacking a right operand" in {
      parse("1 *") must throwA[ParseException]
    }
    
    "accept a division operation" in {
      parse("1 / 2") mustEqual Div(NumLit("1"), NumLit("2"))
    }
    
    "reject a division operation lacking a left operand" in {
      parse("/ 2") must throwA[ParseException]
    }
    
    "reject a division operation lacking a right operand" in {
      parse("1 /") must throwA[ParseException]
    }
    
    "accept a less-than operation" in {
      parse("1 < 2") mustEqual Lt(NumLit("1"), NumLit("2"))
    }
    
    "reject a less-than operation lacking a left operand" in {
      parse("< 2") must throwA[ParseException]
    }
    
    "reject a less-than operation lacking a right operand" in {
      parse("1 <") must throwA[ParseException]
    }
    
    "accept a less-than-equal operation" in {
      parse("1 <= 2") mustEqual LtEq(NumLit("1"), NumLit("2"))
    }
    
    "reject a less-than-equal operation lacking a left operand" in {
      parse("<= 2") must throwA[ParseException]
    }
    
    "reject a less-than-equal operation lacking a right operand" in {
      parse("1 <=") must throwA[ParseException]
    }
    
    "accept a greater-than operation" in {
      parse("1 > 2") mustEqual Gt(NumLit("1"), NumLit("2"))
    }
    
    "reject a greater-than operation lacking a left operand" in {
      parse("> 2") must throwA[ParseException]
    }
    
    "reject a greater-than operation lacking a right operand" in {
      parse("1 >") must throwA[ParseException]
    }
    
    "accept a greater-than-equal operation" in {
      parse("1 >= 2") mustEqual GtEq(NumLit("1"), NumLit("2"))
    }
    
    "reject a greater-than-equal operation lacking a left operand" in {
      parse(">= 2") must throwA[ParseException]
    }
    
    "reject a greater-than-equal operation lacking a right operand" in {
      parse("1 >=") must throwA[ParseException]
    }
    
    "accept a equality operation" in {
      parse("1 = 2") mustEqual Eq(NumLit("1"), NumLit("2"))
    }
    
    "reject an equality operation lacking a left operand" in {
      parse("= 2") must throwA[ParseException]
    }
    
    "reject an equality operation lacking a right operand" in {
      parse("1 =") must throwA[ParseException]
    }
    
    "accept a not-equal operation" in {
      parse("1 != 2") mustEqual NotEq(NumLit("1"), NumLit("2"))
    }
    
    "reject a not-equal operation lacking a left operand" in {
      parse("!= 2") must throwA[ParseException]
    }
    
    "reject a not-equal operation lacking a right operand" in {
      parse("1 !=") must throwA[ParseException]
    }
    
    "accept a boolean and operation" in {
      parse("1 & 2") mustEqual And(NumLit("1"), NumLit("2"))
    }
    
    "reject a boolean and operation lacking a left operand" in {
      parse("& 2") must throwA[ParseException]
    }
    
    "reject a boolean and operation lacking a right operand" in {
      parse("1 &") must throwA[ParseException]
    }
    
    "accept a boolean or operation" in {
      parse("1 | 2") mustEqual Or(NumLit("1"), NumLit("2"))
    }
    
    "reject a boolean or operation lacking a left operand" in {
      parse("| 2") must throwA[ParseException]
    }
    
    "reject a boolean or operation lacking a right operand" in {
      parse("1 |") must throwA[ParseException]
    }
    
    "accept boolean complementation" in {
      parse("!1") mustEqual Comp(NumLit("1"))
    }
    
    "accept numeric negation" in {
      parse("~1") mustEqual Neg(NumLit("1"))
    }
    
    "accept parentheticals" in {
      parse("(1)") mustEqual Paren(NumLit("1"))
    }
    
    "reject unmatched parentheses" in {
      parse("(") must throwA[ParseException]
      parse("(()") must throwA[ParseException]
    }
  }
  
  "operator precedence" should {
    "favor descent/deref over negation/complement" in {
      parse("!1.x") mustEqual Comp(Descent(NumLit("1"), "x"))
      parse("~1.x") mustEqual Neg(Descent(NumLit("1"), "x"))
      parse("!1[2]") mustEqual Comp(Deref(NumLit("1"), NumLit("2")))
      parse("~1[2]") mustEqual Neg(Deref(NumLit("1"), NumLit("2")))
    }
    
    "favor negation/complement over multiplication/division" in {
      parse("!a * b") mustEqual Mul(Comp(Var("a")), Var("b"))
      parse("~a * b") mustEqual Mul(Neg(Var("a")), Var("b"))
      parse("!a / b") mustEqual Div(Comp(Var("a")), Var("b"))
      parse("~a / b") mustEqual Div(Neg(Var("a")), Var("b"))
    }
    
    "favor multiplication/division over addition/subtraction" in {
      parse("a + b * c") mustEqual Add(Var("a"), Mul(Var("b"), Var("c")))
      parse("a - b * c") mustEqual Sub(Var("a"), Mul(Var("b"), Var("c")))
      parse("a * b + c") mustEqual Add(Mul(Var("a"), Var("b")), Var("c"))
      parse("a * b - c") mustEqual Sub(Mul(Var("a"), Var("b")), Var("c"))
      
      parse("a + b / c") mustEqual Add(Var("a"), Div(Var("b"), Var("c")))
      parse("a - b / c") mustEqual Sub(Var("a"), Div(Var("b"), Var("c")))
      parse("a / b + c") mustEqual Add(Div(Var("a"), Var("b")), Var("c"))
      parse("a / b - c") mustEqual Sub(Div(Var("a"), Var("b")), Var("c"))
    }
    
    "favor addition/subtraction over inequality operators" in {
      parse("a < b + c") mustEqual Lt(Var("a"), Add(Var("b"), Var("c")))
      parse("a <= b + c") mustEqual LtEq(Var("a"), Add(Var("b"), Var("c")))
      parse("a + b < c") mustEqual Lt(Add(Var("a"), Var("b")), Var("c"))
      parse("a + b <= c") mustEqual LtEq(Add(Var("a"), Var("b")), Var("c"))
      
      parse("a < b - c") mustEqual Lt(Var("a"), Sub(Var("b"), Var("c")))
      parse("a <= b - c") mustEqual LtEq(Var("a"), Sub(Var("b"), Var("c")))
      parse("a - b < c") mustEqual Lt(Sub(Var("a"), Var("b")), Var("c"))
      parse("a - b <= c") mustEqual LtEq(Sub(Var("a"), Var("b")), Var("c"))
      
      parse("a > b + c") mustEqual Gt(Var("a"), Add(Var("b"), Var("c")))
      parse("a >= b + c") mustEqual GtEq(Var("a"), Add(Var("b"), Var("c")))
      parse("a + b > c") mustEqual Gt(Add(Var("a"), Var("b")), Var("c"))
      parse("a + b >= c") mustEqual GtEq(Add(Var("a"), Var("b")), Var("c"))
      
      parse("a > b - c") mustEqual Gt(Var("a"), Sub(Var("b"), Var("c")))
      parse("a >= b - c") mustEqual GtEq(Var("a"), Sub(Var("b"), Var("c")))
      parse("a - b > c") mustEqual Gt(Sub(Var("a"), Var("b")), Var("c"))
      parse("a - b >= c") mustEqual GtEq(Sub(Var("a"), Var("b")), Var("c"))
    }
    
    "favor inequality operators over equality operators" in {
      parse("a = b < c") mustEqual Eq(Var("a"), Lt(Var("b"), Var("c")))
      parse("a != b < c") mustEqual NotEq(Var("a"), Lt(Var("b"), Var("c")))
      parse("a < b = c") mustEqual Eq(Lt(Var("a"), Var("b")), Var("c"))
      parse("a < b != c") mustEqual NotEq(Lt(Var("a"), Var("b")), Var("c"))
      
      parse("a = b <= c") mustEqual Eq(Var("a"), LtEq(Var("b"), Var("c")))
      parse("a != b <= c") mustEqual NotEq(Var("a"), LtEq(Var("b"), Var("c")))
      parse("a <= b = c") mustEqual Eq(LtEq(Var("a"), Var("b")), Var("c"))
      parse("a <= b != c") mustEqual NotEq(LtEq(Var("a"), Var("b")), Var("c"))
      
      parse("a = b > c") mustEqual Eq(Var("a"), Gt(Var("b"), Var("c")))
      parse("a != b > c") mustEqual NotEq(Var("a"), Gt(Var("b"), Var("c")))
      parse("a > b = c") mustEqual Eq(Gt(Var("a"), Var("b")), Var("c"))
      parse("a > b != c") mustEqual NotEq(Gt(Var("a"), Var("b")), Var("c"))
      
      parse("a = b >= c") mustEqual Eq(Var("a"), GtEq(Var("b"), Var("c")))
      parse("a != b >= c") mustEqual NotEq(Var("a"), GtEq(Var("b"), Var("c")))
      parse("a >= b = c") mustEqual Eq(GtEq(Var("a"), Var("b")), Var("c"))
      parse("a >= b != c") mustEqual NotEq(GtEq(Var("a"), Var("b")), Var("c"))
    }
    
    "favor equality operators over and/or" in {
      parse("a & b = c") mustEqual And(Var("a"), Eq(Var("b"), Var("c")))
      parse("a | b = c") mustEqual Or(Var("a"), Eq(Var("b"), Var("c")))
      parse("a = b & c") mustEqual And(Eq(Var("a"), Var("b")), Var("c"))
      parse("a = b | c") mustEqual Or(Eq(Var("a"), Var("b")), Var("c"))
      
      parse("a & b != c") mustEqual And(Var("a"), NotEq(Var("b"), Var("c")))
      parse("a | b != c") mustEqual Or(Var("a"), NotEq(Var("b"), Var("c")))
      parse("a != b & c") mustEqual And(NotEq(Var("a"), Var("b")), Var("c"))
      parse("a != b | c") mustEqual Or(NotEq(Var("a"), Var("b")), Var("c"))
    }
    
    "favor and/or operators over new" in {
      parse("new a & b") mustEqual New(And(Var("a"), Var("b")))
      parse("new a | b") mustEqual New(Or(Var("a"), Var("b")))
    }
    
    "favor new over where" in {
      parse("new a where b") mustEqual Operation(New(Var("a")), "where", Var("b"))
    }
    
    "favor where over relate" in {
      parse("a where b :: c d") mustEqual Relate(Operation(Var("a"), "where", Var("b")), Var("c"), Var("d"))
      parse("a :: b where c d") mustEqual Relate(Var("a"), Operation(Var("b"), "where", Var("c")), Var("d"))
    }
  }
  
  "operator associativity" should {
    "associate multiplication to the left" in {
      parse("a * b * c") mustEqual Mul(Mul(Var("a"), Var("b")), Var("c"))
    }
    
    "associate division to the left" in {
      parse("a / b / c") mustEqual Div(Div(Var("a"), Var("b")), Var("c"))
    }
    
    "associate addition to the left" in {
      parse("a + b + c") mustEqual Add(Add(Var("a"), Var("b")), Var("c"))
    }
    
    "associate subtraction to the left" in {
      parse("a - b - c") mustEqual Sub(Sub(Var("a"), Var("b")), Var("c"))
    }
    
    "associate less-than to the left" in {
      parse("a < b < c") mustEqual Lt(Lt(Var("a"), Var("b")), Var("c"))
    }
    
    "associate less-than-equal to the left" in {
      parse("a <= b <= c") mustEqual LtEq(LtEq(Var("a"), Var("b")), Var("c"))
    }
    
    "associate greater-than to the left" in {
      parse("a > b > c") mustEqual Gt(Gt(Var("a"), Var("b")), Var("c"))
    }
    
    "associate greater-than-equal to the left" in {
      parse("a >= b >= c") mustEqual GtEq(GtEq(Var("a"), Var("b")), Var("c"))
    }
    
    "associate equal to the left" in {
      parse("a = b = c") mustEqual Eq(Eq(Var("a"), Var("b")), Var("c"))
    }
    
    "associate not-equal to the left" in {
      parse("a != b != c") mustEqual NotEq(NotEq(Var("a"), Var("b")), Var("c"))
    }
    
    "associate where to the left" in {
      parse("a where b where c") mustEqual Operation(Operation(Var("a"), "where", Var("b")), "where", Var("c"))
    }
  }
  
  "whitespace processing" should {
    "skip any amount of leading space characters" in {
      parse(" 1") mustEqual NumLit("1")
      parse("     1") mustEqual NumLit("1")
      parse("\t  \t 1") mustEqual NumLit("1")
      parse("\n\r  ;\r\t  \t\n ;;1") mustEqual NumLit("1")
    }
    
    "skip any amount of trailing space characters" in {
      parse("1 ") mustEqual NumLit("1")
      parse("1     ") mustEqual NumLit("1")
      parse("1\t  \t ") mustEqual NumLit("1")
      parse("1\n\r  ;\r\t  \t\n ;;") mustEqual NumLit("1")
    }
    
    "skip leading line comments delimited by newline" in {
      parse("-- testing one two three\n1") mustEqual NumLit("1")
      parse("-- testing one two three\n--four five six\n1") mustEqual NumLit("1")
      parse("   \t  -- testing one two three\n\n  \t--four five six\n\r  1") mustEqual NumLit("1")
    }
    
    "skip trailing line comments delimited by newline" in {
      parse("1-- testing one two three\n") mustEqual NumLit("1")
      parse("1-- testing one two three\n--four five six\n") mustEqual NumLit("1")
      parse("1   \t  -- testing one two three\n\n  \t--four five six\n\r  ") mustEqual NumLit("1")
    }
    
    "skip leading block comments" in {
      parse("(- testing one two three -)1") mustEqual NumLit("1")
      parse("  (- testing one two three -)   1") mustEqual NumLit("1")
      parse("  (- testing one \n two\n three -)   1") mustEqual NumLit("1")
      parse("(- testing one two three -)\n(-four five six -)\n1") mustEqual NumLit("1")
      parse("(- testing one- \\- two three -)\n(-four \n five-- \t six -)\n1") mustEqual NumLit("1")
      parse("   \t  (- testing one two three -)\n\n  \t(- four five six -)\n\r  1") mustEqual NumLit("1")
    }
    
    "skip trailing line comments delimited by newline" in {
      parse("1(- testing one two three -)") mustEqual NumLit("1")
      parse("1  (- testing one two three -)    ") mustEqual NumLit("1")
      parse("1  (- testing one \n two\n three -)  ") mustEqual NumLit("1")
      parse("1\t  (- testing one two three -)\n(-four five six -)\n") mustEqual NumLit("1")
      parse("1(- testing one- \\- two three -)\n(-four \n five-- \t six -)\n") mustEqual NumLit("1")
      parse("1   \t  (- testing one two three -)\n\n  \t(- four five six -)\n\r  ") mustEqual NumLit("1")
    }
  }
  
  "composed expression parsing" should {
    "parse a no param function containing a parenthetical" in {
      parse("a := 1 (2)") mustEqual Binding("a", Vector(), NumLit("1"), Paren(NumLit("2")))
    }
    
    "parse a no param function containing a no param function" in {
      parse("a := 1 c := 2 3") mustEqual Binding("a", Vector(), NumLit("1"), Binding("c", Vector(), NumLit("2"), NumLit("3")))
    }
    
    "parse a no param function containing a 1 param function" in {
      parse("a := 1 c('d) := 2 3") mustEqual Binding("a", Vector(), NumLit("1"), Binding("c", Vector("'d"), NumLit("2"), NumLit("3")))
    }
    
    "correctly nest multiple binds" in {
      val input = """
        | a :=
        |   b := dataset(//f)
        |   c := dataset(//g)
        |
        |   d
        | e""".stripMargin
      
      parse(input) mustEqual Binding("a", Vector(), Binding("b", Vector(), Dispatch("dataset", Vector(StrLit("/f"))), Binding("c", Vector(), Dispatch("dataset", Vector(StrLit("/g"))), Var("d"))), Var("e"))
    }
  }
  
  "specification examples" >> {
    "deviant-durations.qrl" >> {
      val input = """
        | interactions := dataset(//interactions)
        | 
        | big1z('userId) :=
        |   userInteractions := interactions where interactions.userId = 'userId
        |   
        |   m := mean(userInteractions.duration)
        |   sd := stdDev(userInteractions.duration)
        | 
        |   {
        |     userId: 'userId,
        |     interaction: userInteractions where userInteractions.duration > m + (sd * 3)
        |   }
        |   
        | big1z
        """.stripMargin
      
      parse(input) must not(throwA[ParseException])
    }
    
    "first-conversion.qrl" >> {
      val input = """
        | firstConversionAfterEachImpression('userId) :=
        |   clicks'      := dataset(//clicks)
        |   conversions' := dataset(//conversions)
        |   impressions' := dataset(//impressions)
        | 
        |   clicks      := clicks' where clicks'.userId = 'userId
        |   conversions := conversions' where conversions'.userId = 'userId
        |   impressions := impressions' where impressions'.userId = 'userId
        | 
        |   greaterConversions('time) :=
        |     impressionTimes := impressions where impressions.time = 'time
        |     conversionTimes :=
        |       conversions where conversions.time = min(conversions where conversions.time > 'time).time
        |     
        |     conversionTimes :: impressionTimes
        |       { impression: impressions, nextConversion: conversions }
        | 
        |   greaterConversions
        | 
        | firstConversionAfterEachImpression
        """.stripMargin
      
      parse(input) must not(throwA[ParseException])
    }
    
    "histogram.qrl" >> {
      val input = """
        | clicks := dataset(//clicks)
        | 
        | histogram('value) :=
        |   { cnt: count(clicks where clicks = 'value), value: 'value }
        |   
        | histogram
        """.stripMargin
      
      parse(input) must not(throwA[ParseException])
    }
    
    "interaction-totals.qrl" >> {
      val input = """
        | interactions := dataset(//interactions)
        | total('hour, 'day) :=
        |   sum(hourOfDay((interactions where dayOfWeek(interactions.time) = 'day).time) = 'hour)
        |   
        | total
        """.stripMargin
      
      parse(input) must not(throwA[ParseException])
    }
    
    "relative-durations.qrl" >> {
      val input = """
        | interactions := dataset(//interactions)
        | 
        | relativeDurations('userId, 'value) :=
        |   userInteractions := interactions where interactions.userId = 'userId
        |   interactionDurations := (userInteractions where userInteractions = 'value).duration
        |   totalDurations := sum(userInteractions.duration)
        | 
        |   { userId: 'userId, ratio: interactionDurations / totalDurations }
        | 
        | relativeDurations
        """.stripMargin
      
      parse(input) must not(throwA[ParseException])
    }
  }
  
  "global ambiguity resolution" should {
    "associate paired consecutive parentheses" in {
      val expected = 
        Binding("a", Vector(),
          Binding("b", Vector(),
            Var("c"), Paren(Var("d"))),
          Paren(Var("e")))
      
      parse("a := b := c (d) (e)") mustEqual expected
    }
  }
  
  def parse(str: String): Tree = parse(LineStream(str))
}
