package com.precog.quirrel
package parser

import com.codecommit.gll.LineStream
import org.scalacheck.Prop
import org.specs2.ScalaCheck
import org.specs2.mutable._

import java.io.File
import scala.io.Source

object ParserSpecs extends Specification with ScalaCheck with StubPhases with Parser {
  import ast._
  
  val keywords = Set(
    "new",
    "true",
    "false",
    "where",
    "with",
    "union",
    "intersect",
    "difference",
    "neg",
    "undefined",
    "null",
    "import",
    "solve",
    "if",
    "then",
    "else",
    "assert")
  
  "uncomposed expression parsing" should {
    "accept parameterized bind with one parameter" in {
      parseSingle("x(a) := 1 2") must beLike {
        case Let(_, Identifier(Vector(), "x"), Vector("a"), NumLit(_, "1"), NumLit(_, "2")) => ok
      }
    }
    
    "reject parameterized bind with no parameters" in {
      parseSingle("x() := 1 2") must throwA[ParseException]
    }
    
    "reject parameterized bind with one missing expression" in {
      parseSingle("x(a) := 1") must throwA[ParseException]
    }
    
    "reject parameterized bind with two missing expressions" in {
      parseSingle("x(a) :=") must throwA[ParseException]
    }
    
    "accept parameterized bind with multiple parameter" in {
      parseSingle("x(a, b, c) := 1 2") must beLike {
        case Let(_, Identifier(Vector(), "x"), Vector("a", "b", "c"), NumLit(_, "1"), NumLit(_, "2")) => ok
      }
    }
    
    "accept unparameterized bind" in {
      parseSingle("x := 1 2") must beLike {
        case Let(_, Identifier(Vector(), "x"), Vector(), NumLit(_, "1"), NumLit(_, "2")) => ok
      }
    }
    
    "reject unparameterized bind with one missing expression" in {
      parseSingle("x := 1") must throwA[ParseException]
    }
    
    "reject unparameterized bind with two missing expressions" in {
      parseSingle("x :=") must throwA[ParseException]
    }
    
    "accept a specific import expression" in {
      parseSingle("import std 42") must beLike {
        case Import(_, SpecificImport(Vector("std")), NumLit(_, "42")) => ok
      }
      
      parseSingle("import std::math::alissa 42") must beLike {
        case Import(_, SpecificImport(Vector("std", "math", "alissa")), NumLit(_, "42")) => ok
      }
    }
    
    "accept a wildcard import expression" in {
      parseSingle("import std::* 42") must beLike {
        case Import(_, WildcardImport(Vector("std")), NumLit(_, "42")) => ok
      }
      
      parseSingle("import std::math::alissa::* 42") must beLike {
        case Import(_, WildcardImport(Vector("std", "math", "alissa")), NumLit(_, "42")) => ok
      }
    }

    "accept a wildcard import followed by a let" in {
      parseSingle("""
        | import std::time::*
        | foo := //foo
        | foo""".stripMargin) must beLike {
        case Import(_, WildcardImport(Vector("std", "time")), _) => ok
      }
    }

    "accept a let followed by a wildcard import" in {
      parseSingle("""
        | foo :=
        |   import std::time::*
        |   //foo
        | foo""".stripMargin) must beLike {
        case Let(_, _, _, Import(_, WildcardImport(Vector("std", "time")), _), _) => ok
      }
    }

    "accept a wildcard import followed by a distinct" in {
      parseSingle("""
        | import std::time::*
        | distinct(//foo)""".stripMargin) must beLike {
        case Import(_, WildcardImport(Vector("std", "time")), _) => ok
      }
    }
    
    "reject a singular wildcard import expression" in {
      parseSingle("import _ 42") must throwA[ParseException]
    }
    
    "accept a simple assertion" in {
      parseSingle("assert true 42") must beLike {
        case Assert(_, BoolLit(_, true), NumLit(_, "42")) => ok
      }
    }
    
    "accept an assertion with a compound expression" in {
      parseSingle("assert a < 12 [a]") must beLike {
        case Assert(_,
          Lt(_,
            Dispatch(_, Identifier(Vector(), "a"), Vector()),
            NumLit(_, "12")),
          ArrayDef(_, Vector(Dispatch(_, Identifier(Vector(), "a"), Vector())))) => ok
      }
    }
    
    "reject an assertion lacking a predicate" in {
      parseSingle("assert 42") must throwA[ParseException]
    }

    "accept a single solve expression" in {
      parseSingle("solve 'a 'a + 42") must beLike {
        case Solve(_, Vector(TicVar(_, "'a")), Add(_, TicVar(_, "'a"), NumLit(_, "42"))) => ok
      }
    }

    "accept a solve expression with two tic variables" >> {
      "with Add" >> {
        parseSingle("solve 'a, 'b 'a + 'b") must beLike {
          case Solve(_, Vector(TicVar(_, "'a"), TicVar(_, "'b")), Add(_, TicVar(_, "'a"), TicVar(_, "'b"))) => ok
        }
      }

      "with Union" >> {
        parseSingle("solve 'a, 'b 'a union 'b") must beLike {
          case Solve(_, Vector(TicVar(_, "'a"), TicVar(_, "'b")), Union(_, TicVar(_, "'a"), TicVar(_, "'b"))) => ok
        }
      }

      "with Difference" >> {
        parseSingle("solve 'a, 'b 'a difference 'b") must beLike {
          case Solve(_, Vector(TicVar(_, "'a"), TicVar(_, "'b")), Difference(_, TicVar(_, "'a"), TicVar(_, "'b"))) => ok
        }
      }

      "with And, Where" >> {
        parseSingle("solve 'a, 'b ('a where true) & ('b where false)") must beLike {
          case Solve(_, Vector(TicVar(_, "'a"), TicVar(_, "'b")), And(_, Paren(_, Where(_, TicVar(_, "'a"), BoolLit(_, true))), Paren(_, Where(_, TicVar(_, "'b"), BoolLit(_, false))))) => ok
        }
      }
    }
    
    "accept a solve expression with a single expression constraint" in {
      parseSingle("solve 'a = 12 + true 1") must beLike {
        case Solve(_, Vector(Eq(_, TicVar(_, "'a"), Add(_, NumLit(_, "12"), BoolLit(_, true)))), NumLit(_, "1")) => ok
      }
    }
    
    "accept a solve expression with a single expression constraint and one tic variable" in {
      parseSingle("solve 'a = 12 + true, 'b 1") must beLike {
        case Solve(_, Vector(Eq(_, TicVar(_, "'a"), Add(_, NumLit(_, "12"), BoolLit(_, true))), TicVar(_, "'b")), NumLit(_, "1")) => ok
      }
    }
    
    "accept a solve expression with a nested solve expression as a constraint" in {
      parseSingle("solve solve 'a 1 2") must beLike {
        case Solve(_, Vector(Solve(_, Vector(TicVar(_, "'a")), NumLit(_, "1"))), NumLit(_, "2")) => ok
      }
    }
    
    "accept a solve expression with a nested solve expression with two tic variables as a constraint" in {
      parseSingle("solve solve 'a, 'b 1 2") must beLike {
        case Solve(_, Vector(Solve(_, Vector(TicVar(_, "'a"), TicVar(_, "'b")), NumLit(_, "1"))), NumLit(_, "2")) => ok
      }
    }
    
    "accept a solve expression followed by a let" in {
      parseSingle("solve 'a foo(b) := 1 + 'a foo") must beLike {
        case Solve(_, Vector(TicVar(_, "'a")), Let(_, Identifier(Vector(), "foo"), Vector("b"), Add(_, NumLit(_, "1"), TicVar(_, "'a")), Dispatch(_, Identifier(Vector(), "foo"), Vector()))) => ok
      }
    }
    
    "accept a let expression without a parameter followed by a solve" in {
      parseSingle("foo := (solve 'b 10 + 'b) foo") must beLike {
        case Let(_, Identifier(Vector(), "foo"), Vector(), Paren(_, Solve(_, Vector(TicVar(_, "'b")), Add(_, NumLit(_, "10"), TicVar(_, "'b")))), Dispatch(_, Identifier(Vector(), "foo"), Vector())) => ok
      }
    }
    
    "accept a let expression with a parameter followed by a solve" in {
      parseSingle("foo(a) := (solve 'b 'a + 'b )foo") must beLike {
        case Let(_, Identifier(Vector(), "foo"), Vector("a"), Paren(_, Solve(_, Vector(TicVar(_, "'b")), Add(_, TicVar(_, "'a"), TicVar(_, "'b")))), Dispatch(_, Identifier(Vector(), "foo"), Vector())) => ok
      }
    }
        
    "accept a let expression followed by a solve with no parens around the solve" in {
      parseSingle("foo := solve 'b 'b foo") must beLike {
        case Let(_, Identifier(Vector(), "foo"), Vector(), Solve(_, Vector(TicVar(_, "'b")), TicVar(_, "'b")), Dispatch(_, Identifier(Vector(), "foo"), Vector())) => ok
      }
    }
    
    "disambiguate solve and let" in {
      parseSingle("solve 'a foo(b) := (solve 'c 'b + 'c) foo + 'a") must beLike {
        case Solve(_, Vector(TicVar(_, "'a")), Let(_, Identifier(Vector(), "foo"), Vector("b"), Paren(_, Solve(_, Vector(TicVar(_, "'c")), Add(_, TicVar(_, "'b"), TicVar(_, "'c")))), Add(_, Dispatch(_, Identifier(Vector(), "foo"), Vector()), TicVar(_, "'a")))) => ok
      }
    }

    "accept a 'new' expression" in {
      parseSingle("new 1") must beLike {
        case New(_, NumLit(_, "1")) => ok
      }
    }    

    "accept a 'new' expression followed by a let" in {
      parseSingle("new foo := //foo foo") must beLike {
        case New(_, _) => ok
      }
    }

    "accept a 'new' expression followed by a solve" in {
      parseSingle("new solve 'a 'a") must beLike {
        case New(_, _) => ok
      }
    }
    
    "accept a relate expression" in {
      parseSingle("1 ~ 2 3") must beLike {
        case Relate(_, NumLit(_, "1"), NumLit(_, "2"), NumLit(_, "3")) => ok
      }
    }    

    "accept a relate expression followed by a let" in {
      parseSingle("1 ~ 2 foo := //foo foo") must beLike {
        case Relate(_, NumLit(_, "1"), NumLit(_, "2"), Let(_, Identifier(Vector(), "foo"), Vector(), PathLit("/foo"), Dispatch(_, Identifier(Vector(), "foo"), Vector()))) => ok
      }
    }
    
    "accept a relate expression with more than two constraint sets" in {
      parseSingle("1 ~ 2 ~ 3 ~ 4 5") must beLike {
        case Relate(_, NumLit(_, "1"), NumLit(_, "2"),
          Relate(_, NumLit(_, "2"), NumLit(_, "3"),
            Relate(_, NumLit(_, "3"), NumLit(_, "4"), NumLit(_, "5")))) => ok
      }
    }
    
    "accept a variable without a namespace" in {
      parseSingle("x") must beLike { case Dispatch(_, Identifier(Vector(), "x"), Vector()) => ok }
      parseSingle("cafe_Babe__42_") must beLike { case Dispatch(_, Identifier(Vector(), "cafe_Babe__42_"), Vector()) => ok }
      parseSingle("x'") must beLike { case Dispatch(_, Identifier(Vector(), "x'"), Vector()) => ok }
    }

    "reject a variable named as a keyword" in {
      forall(keywords) { kw =>
        parse("%s := 1 1".format(kw)) must throwA[ParseException]
      }
    }
    
    "reject a variable starting with a number" in {
      parseSingle("2x") must throwA[ParseException]
      parseSingle("123cafe_Babe__42_") must throwA[ParseException]
      parseSingle("4x'") must throwA[ParseException]
    }
    
    "accept a variable with a namespace" in {
      parseSingle("a :: b :: c") must beLike { case Dispatch(_, Identifier(Vector("a", "b"), "c"), Vector()) => ok }
    }

    "reject a variable starting with the namespace operator" in {
      parseSingle(":: a :: b") must throwA[ParseException]
    }

    "reject a variable with a namespace that includes a keyword" in {
      parseSingle("a :: true :: b") must throwA[ParseException]
      parseSingle("a :: b :: false") must throwA[ParseException]
    }

    "reject a variable with a namespace that includes an id starting with a number" in {
      parseSingle("a :: 2x :: b :: c") must throwA[ParseException]
    }

    "accept a tic-variable" in {
      parseSingle("'x") must beLike { case TicVar(_, "'x") => ok }
      parseSingle("'cafe_Babe__42_") must beLike { case TicVar(_, "'cafe_Babe__42_") => ok }
      parseSingle("'x'") must beLike { case TicVar(_, "'x'") => ok }
    }
    
    "reject a tic-variable where the second character is a '" in {
      parseSingle("''x") must throwA[ParseException]
      parseSingle("''cafe_Babe__42_") must throwA[ParseException]
      parseSingle("''x'") must throwA[ParseException]
    }
    
    "accept a path literal" in {
      // TODO find a way to use LoadId instead
      parseSingle("//foo") must beLike { case PathLit("/foo") => ok }
      parseSingle("//foo/bar/baz") must beLike { case PathLit("/foo/bar/baz") => ok }
      parseSingle("//cafe-babe42_silly/SILLY") must beLike { case PathLit("/cafe-babe42_silly/SILLY") => ok }
    }
    
    "accept a relative path literal" in {
      // TODO find a way to use LoadId instead
      parseSingle("./foo") must beLike { case RelPathLit("foo") => ok }
      parseSingle("./foo/bar/baz") must beLike { case RelPathLit("foo/bar/baz") => ok }
      parseSingle("./cafe-babe42_silly/SILLY") must beLike { case RelPathLit("cafe-babe42_silly/SILLY") => ok }
    }
    
    "accept a string literal" in {
      parseSingle("\"I have a dream\"") must beLike { case StrLit(_, "I have a dream") => ok }
      parseSingle("\"\"") must beLike { case StrLit(_, "") => ok }
    }
    
    "reject a string literal that wraps onto a newline" in {
      parseSingle("\"\n\"") must throwA[ParseException]
      parseSingle("\"testing\n\"") must throwA[ParseException]
      parseSingle("\"\none two three\"") must throwA[ParseException]
      parseSingle("\"testing\none two three\"") must throwA[ParseException]
    }
    
    "reject an unterminated string literal" in {
      parseSingle("\"") must throwA[ParseException]
      parseSingle("\"testing") must throwA[ParseException]
    }
    
    "resolve all escape sequences in string literals" in {
      parseSingle("\"\\\"\"") must beLike { case StrLit(_, "\"") => ok }
      parseSingle("\"\\n\"") must beLike { case StrLit(_, "\n") => ok }
      parseSingle("\"\\r\"") must beLike { case StrLit(_, "\r") => ok }
      parseSingle("\"\\f\"") must beLike { case StrLit(_, "\f") => ok }
      parseSingle("\"\\t\"") must beLike { case StrLit(_, "\t") => ok }
      parseSingle("\"\\0\"") must beLike { case StrLit(_, "\0") => ok }
      parseSingle("\"\\\\\"") must beLike { case StrLit(_, "\\") => ok }
    }
    
    "accept a number literal" in {
      parseSingle("1") must beLike { case NumLit(_, "1") => ok }
      parseSingle("256") must beLike { case NumLit(_, "256") => ok }
      parseSingle("256.715") must beLike { case NumLit(_, "256.715") => ok }
      parseSingle("3.1415") must beLike { case NumLit(_, "3.1415") => ok }
      parseSingle("2.7183e26") must beLike { case NumLit(_, "2.7183e26") => ok }
      parseSingle("2.7183E26") must beLike { case NumLit(_, "2.7183E26") => ok }
    }
    
    "reject a number literal ending with a ." in {
      parseSingle("42.") must throwA[ParseException]
    }
    
    "accept a boolean literal" in {
      parseSingle("true") must beLike { case BoolLit(_, true) => ok }
      parseSingle("false") must beLike { case BoolLit(_, false) => ok }
    }    

    "accept an undefined literal" in {
      parseSingle("undefined") must beLike { case UndefinedLit(_) => ok }
    }

    "accept a null literal" in {
      parseSingle("null") must beLike { case NullLit(_) => ok }
    }
    
    "accept an object definition with no properties" in {
      parseSingle("{}") must beLike { case ObjectDef(_, Vector()) => ok }
    }
    
    "reject an object definition with a spurious delimiter" in {
      parseSingle("{,}") must throwA[ParseException]
    }
    
    "reject an unterminated object definition" in {
      parseSingle("{") must throwA[ParseException]
      parseSingle("{ a: 1") must throwA[ParseException]
      parseSingle("{ a: 1,") must throwA[ParseException]
      parseSingle("{ a: 1, b: 2, cafe: 3, star_BUckS: 4") must throwA[ParseException]
      parseSingle("{ a: 1, b: 2, cafe: 3, star_BUckS: 4,") must throwA[ParseException]
    }
    
    "reject an object definition with expr content" in {
      parseSingle("{ 42 }") must throwA[ParseException]
      parseSingle("{ true }") must throwA[ParseException]
      parseSingle("{ \"fourty-two\" }") must throwA[ParseException]
      parseSingle("{{}}") must throwA[ParseException]
    }
    
    "accept an object definition with one property" in {
      parseSingle("{ x: 1 }") must beLike { case ObjectDef(_, Vector(("x", NumLit(_, "1")))) => ok }
      parseSingle("{ \"x\": 1 }") must beLike { case ObjectDef(_, Vector(("x", NumLit(_, "1")))) => ok }
    }
    
    "reject an object definition with a trailing delimiter" in {
      parseSingle("{ x: 1, }") must throwA[ParseException]
      parseSingle("{ \"x\": 1, }") must throwA[ParseException]
    }
    
    "accept an object definition with multiple properties" in {
      parseSingle("{ a: 1, b: 2, cafe: 3, star_BUckS: 4 }") must beLike {
        case ObjectDef(_, Vector(("a", NumLit(_, "1")), ("b", NumLit(_, "2")), ("cafe", NumLit(_, "3")), ("star_BUckS", NumLit(_, "4")))) => ok
      }
      
      parseSingle("{ \"a\": 1, \"b\": 2, \"cafe\": 3, \"star_BUckS\": 4 }") must beLike {
        case ObjectDef(_, Vector(("a", NumLit(_, "1")), ("b", NumLit(_, "2")), ("cafe", NumLit(_, "3")), ("star_BUckS", NumLit(_, "4")))) => ok
      }
    }    

    "accept an object definition with a null property" in {
      parseSingle("{ a: 1, b: 2, cafe: { foo: null }, star_BUckS: null }") must beLike {
        case ObjectDef(_, Vector(("a", NumLit(_, "1")), ("b", NumLit(_, "2")), ("cafe", ObjectDef(_, Vector(("foo", NullLit(_))))), ("star_BUckS", NullLit(_)))) => ok
      }
      
      parseSingle("{ \"a\": 1, \"b\": 2, \"cafe\": { \"foo\": null }, \"star_BUckS\": null }") must beLike {
        case ObjectDef(_, Vector(("a", NumLit(_, "1")), ("b", NumLit(_, "2")), ("cafe", ObjectDef(_, Vector(("foo", NullLit(_))))), ("star_BUckS", NullLit(_)))) => ok
      }
    }
    
    "reject an object definition with undelimited properties" in {
      parseSingle("{ a: 1, b: 2 cafe: 3, star_BUckS: 4 }") must throwA[ParseException]
      parseSingle("{ \"a\": 1, \"b\": 2 \"cafe\": 3, \"star_BUckS\": 4 }") must throwA[ParseException]
      parseSingle("{ a: 1 b: 2 cafe: 3 star_BUckS: 4 }") must throwA[ParseException]
      parseSingle("{ \"a\": 1 \"b\": 2 \"cafe\": 3 \"star_BUckS\": 4 }") must throwA[ParseException]
    }
    
    "accept an object definition with backtic-delimited properties" in {
      parseSingle("{ `$see! what I can do___`: 1, `test \\` ing \\\\ with $%^&*!@#$ me!`: 2 }") must beLike {
        case ObjectDef(_, Vector(("$see! what I can do___", NumLit(_, "1")), ("test ` ing \\ with $%^&*!@#$ me!", NumLit(_, "2")))) => ok
      }
    }
    
    "accept an object definition with quote-delimited properties containing special characters" in {
      parseSingle("{ \"$see! what I can do___\": 1, \"test \\\" ing \\\\ with $%^&*!@#$ me!\": 2 }") must beLike {
        case ObjectDef(_, Vector(("$see! what I can do___", NumLit(_, "1")), ("test \" ing \\ with $%^&*!@#$ me!", NumLit(_, "2")))) => ok
      }
    }

    "accept an array definition with no actuals" in {
      parseSingle("[]") must beLike { case ArrayDef(_, Vector()) => ok }
    }
    
    "reject an array definition with a spurious delimiter" in {
      parseSingle("[,]") must throwA[ParseException]
    }
    
    "reject an unterminated array definition" in {
      parseSingle("[") must throwA[ParseException]
      parseSingle("[1") must throwA[ParseException]
      parseSingle("[1,") must throwA[ParseException]
      parseSingle("[1, 2, 3, 4") must throwA[ParseException]
      parseSingle("[1, 2, 3, 4,") must throwA[ParseException]
    }
    
    "accept an array definition with one actual" in {
      parseSingle("[1]") must beLike { case ArrayDef(_, Vector(NumLit(_, "1"))) => ok }
    }
    
    "accept an array definition with multiple actuals" in {
      parseSingle("[1, 2, 3]") must beLike {
        case ArrayDef(_, Vector(NumLit(_, "1"), NumLit(_, "2"), NumLit(_, "3"))) => ok
      }
    }
    
    "reject undelimited array definitions" in {
      parseSingle("[1, 2 3]") must throwA[ParseException]
      parseSingle("[1 2, 3]") must throwA[ParseException]
      parseSingle("[1 2 3]") must throwA[ParseException]
    }
    
    "accept a property descent" in {
      parseSingle("1.foo") must beLike { case Descent(_, NumLit(_, "1"), "foo") => ok }
      parseSingle("1.e42") must beLike { case Descent(_, NumLit(_, "1"), "e42") => ok }
    }
    
    "reject property descent with invalid property" in {
      parseSingle("1.-sdf") must throwA[ParseException]
      parseSingle("1.42lkj") must throwA[ParseException]
    }
    
    "accept a property descent with a backtic-delimited property" in {
      parseSingle("1.`$see! what I can do___`") must beLike { case Descent(_, NumLit(_, "1"), "$see! what I can do___") => ok }
      parseSingle("1.`test \\` ing \\\\ with $%^&*!@#$ me!`") must beLike { case Descent(_, NumLit(_, "1"), "test ` ing \\ with $%^&*!@#$ me!") => ok }
    }
    
    "accept a metadata descent" in {
      parseSingle("1@foo") must beLike { case MetaDescent(_, NumLit(_, "1"), "foo") => ok }
      parseSingle("1@e42") must beLike { case MetaDescent(_, NumLit(_, "1"), "e42") => ok }
    }
    
    "reject metadta descent with invalid property" in {
      parseSingle("1@-sdf") must throwA[ParseException]
      parseSingle("1@42lkj") must throwA[ParseException]
    }
    
    "accept a metadata descent with a backtic-delimited property" in {
      parseSingle("1@`$see! what I can do___`") must beLike { case MetaDescent(_, NumLit(_, "1"), "$see! what I can do___") => ok }
      parseSingle("1@`test \\` ing \\\\ with $%^&*!@#$ me!`") must beLike { case MetaDescent(_, NumLit(_, "1"), "test ` ing \\ with $%^&*!@#$ me!") => ok }
    }
    
    "accept an array dereference" in {
      parseSingle("1[2]") must beLike { case Deref(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
      parseSingle("x[y]") must beLike { case Deref(_, Dispatch(_, Identifier(Vector(), "x"), Vector()), Dispatch(_, Identifier(Vector(), "y"), Vector())) => ok }
    }
    
    "reject an array dereference with multiple indexes" in {
      parseSingle("1[1, 2]") must throwA[ParseException]
    }
    
    "reject an array dereference with no indexes" in {
      parseSingle("1[]") must throwA[ParseException]
    }
    
    "reject a dispatch on wildcard" in {
      parseSingle("_(42)") must throwA[ParseException]
    }
    
    "accept a dispatch with one actual" in {
      parseSingle("x(1)") must beLike { case Dispatch(_, Identifier(Vector(), "x"), Vector(NumLit(_, "1"))) => ok }
    }
    
    "reject a dispatch with no actuals" in {
      parseSingle("x()") must throwA[ParseException]
    }

    "accept a dispatch with one actual and a namespace" in {
      parseSingle("a :: b :: c :: d(1)") must beLike { case Dispatch(_, Identifier(Vector("a", "b", "c"), "d"), Vector(NumLit(_, "1"))) => ok }
    }

    "reject a dispatch with no actuals and a namespace" in {
      parseSingle("a :: b()") must throwA[ParseException]
    }
    
    "reject a dispatch with undelimited actuals" in {
      parseSingle("x(1, 2 3)") must throwA[ParseException]
      parseSingle("x(1 2, 3)") must throwA[ParseException]
      parseSingle("x(1 2 3)") must throwA[ParseException]
    }

    "reject a dispatch with undelimited actuals and a namespace" in {
      parseSingle("x :: y(1, 2 3)") must throwA[ParseException]
      parseSingle("x :: y(1 2, 3)") must throwA[ParseException]
      parseSingle("x :: y(1 2 3)") must throwA[ParseException]
    }
    
    "reject a dispatch with one actual and named as a keyword" in {
      parseSingle("true(1)") must throwA[ParseException]
      parseSingle("false(1)") must throwA[ParseException]
      parseSingle("null(1)") must throwA[ParseException]
    }

    "reject a dispatch with one actual and a namespace with a keyword" in {
      parseSingle("true :: x :: y(1)") must throwA[ParseException]
      parseSingle("a :: b :: false(1)") must throwA[ParseException]
      parseSingle("a :: null(b) :: c") must throwA[ParseException]
    }
    
    "accept a dispatch with multiple actuals" in {
      parseSingle("x(1, 2, 3)") must beLike {
        case Dispatch(_, Identifier(Vector(), "x"), Vector(NumLit(_, "1"), NumLit(_, "2"), NumLit(_, "3"))) => ok
      }
    }

    "accept a dispatch with multiple actuals and a namespace" in {
      parseSingle("x :: y :: z :: quirky(1, 2, 3)") must beLike {
        case Dispatch(_, Identifier(Vector("x", "y", "z"), "quirky"), Vector(NumLit(_, "1"), NumLit(_, "2"), NumLit(_, "3"))) => ok
      }
    }
    
    "reject a dispatch with multiple actuals named as a keyword" in {
      parseSingle("new(1, 2, 3)") must throwA[ParseException]
      parseSingle("true(1, 2, 3)") must throwA[ParseException]
      parseSingle("false(1, 2, 3)") must throwA[ParseException]
      parseSingle("null(1, 2, 3)") must throwA[ParseException]
    }

    "reject a dispatch with multiple actuals named as a keyword and a namespace" in {
      parseSingle("new :: a :: b(1, 2, 3)") must throwA[ParseException]
      parseSingle("a :: true :: b(1, 2, 3)") must throwA[ParseException]
      parseSingle("a :: b :: false(1, 2, 3)") must throwA[ParseException]
    }
    
    "accept an infix operation with numerics as left and right" >> {
      "where" >> {
        parseSingle("1 where 2") must beLike {
          case Where(_, NumLit(_, "1"), NumLit(_, "2")) => ok
        }
      }
      
      "with" >> {
        parseSingle("1 with 2") must beLike {
          case With(_, NumLit(_, "1"), NumLit(_, "2")) => ok
        }
      }
      "union" >> {
        parseSingle("1 union 2") must beLike {
          case Union(_, NumLit(_, "1"), NumLit(_, "2")) => ok
        }
      }
      "intersect" >> {
        parseSingle("1 intersect 2") must beLike {
          case Intersect(_, NumLit(_, "1"), NumLit(_, "2")) => ok
        }
      }      
      "difference" >> {
        parseSingle("1 difference 2") must beLike {
          case Difference(_, NumLit(_, "1"), NumLit(_, "2")) => ok
        }
      }
    }
    
    "accept an infix operation with null and strings" >> {
      "where" >> {
        parseSingle("""null where "foo" """) must beLike {
          case Where(_, NullLit(_), StrLit(_, "foo")) => ok
        }
      }
      
      "with" >> {
        parseSingle(""""foo" with null""") must beLike {
          case With(_, StrLit(_, "foo"), NullLit(_)) => ok
        }
      }
      "union" >> {
        parseSingle("""null union "foo" """) must beLike {
          case Union(_, NullLit(_), StrLit(_, "foo")) => ok
        }
      }
      "intersect" >> {
        parseSingle(""""foo" intersect null""") must beLike {
          case Intersect(_, StrLit(_, "foo"), NullLit(_)) => ok
        }
      }     
      "intersect" >> {
        parseSingle(""""foo" difference null""") must beLike {
          case Difference(_, StrLit(_, "foo"), NullLit(_)) => ok
        }
      }
    }
    
    "reject an infix operation *not* named where" in {
      parseSingle("1 x 2") must throwA[ParseException]
      parseSingle("1 caFE_BABE42__ 2") must throwA[ParseException]
      parseSingle("1 new 2") must throwA[ParseException]
      parseSingle("1 true 2") must throwA[ParseException]
      parseSingle("1 false 2") must throwA[ParseException]
    }
    
    "reject an infix operation lacking a left operand" in {
      parseSingle("1 blah") must throwA[ParseException]
    }
    
    "reject an infix operation lacking a right operand" in {
      parseSingle("blah 1") must throwA[ParseException]
    }
    
    "accept an if/else operation" in {
      parseSingle("if true then 2 else 3") must beLike {
        case Cond(_, BoolLit(_, true), NumLit(_, "2"), NumLit(_, "3")) => ok
      }
    }

    "accept an addition operation" in {
      parseSingle("1 + 2") must beLike { case Add(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject an addition operation lacking a left operand" in {
      parseSingle("+ 2") must throwA[ParseException]
    }
    
    "reject an addition operation lacking a right operand" in {
      parseSingle("1 +") must throwA[ParseException]
    }
    
    "accept a subtraction operation" in {
      parseSingle("1 - 2") must beLike { case Sub(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a subtraction operation lacking a left operand" in {
      parseSingle("- 2") must throwA[ParseException]
    }
    
    "reject a subtraction operation lacking a right operand" in {
      parseSingle("1 -") must throwA[ParseException]
    }
    
    "accept a multiplication operation" in {
      parseSingle("1 * 2") must beLike { case Mul(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a multiplication operation lacking a left operand" in {
      parseSingle("* 2") must throwA[ParseException]
    }
    
    "reject a multiplication operation lacking a right operand" in {
      parseSingle("1 *") must throwA[ParseException]
    }
    
    "accept a division operation" in {
      parseSingle("1 / 2") must beLike { case Div(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a division operation lacking a left operand" in {
      parseSingle("/ 2") must throwA[ParseException]
    }
    
    "reject a division operation lacking a right operand" in {
      parseSingle("1 /") must throwA[ParseException]
    }
    
    "accept a mod operation" in {
      parseSingle("1 % 2") must beLike { case Mod(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a mod operation lacking a left operand" in {
      parseSingle("% 2") must throwA[ParseException]
    }
    
    "reject a division operation lacking a right operand" in {
      parseSingle("1 %") must throwA[ParseException]
    }
    
    "accept a power operation" in {
      parseSingle("1 ^ 2") must beLike { case Pow(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }

    "reject a power operation lacking a left operand" in {
      parseSingle("^ 1") must throwA[ParseException]
    }

    "reject a power operation lacking a right operand" in {
      parseSingle("1 ^") must throwA[ParseException]
    }

    "accept a less-than operation" in {
      parseSingle("1 < 2") must beLike { case Lt(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a less-than operation lacking a left operand" in {
      parseSingle("< 2") must throwA[ParseException]
    }
    
    "reject a less-than operation lacking a right operand" in {
      parseSingle("1 <") must throwA[ParseException]
    }
    
    "accept a less-than-equal operation" in {
      parseSingle("1 <= 2") must beLike { case LtEq(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a less-than-equal operation lacking a left operand" in {
      parseSingle("<= 2") must throwA[ParseException]
    }
    
    "reject a less-than-equal operation lacking a right operand" in {
      parseSingle("1 <=") must throwA[ParseException]
    }
    
    "accept a greater-than operation" in {
      parseSingle("1 > 2") must beLike { case Gt(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a greater-than operation lacking a left operand" in {
      parseSingle("> 2") must throwA[ParseException]
    }
    
    "reject a greater-than operation lacking a right operand" in {
      parseSingle("1 >") must throwA[ParseException]
    }
    
    "accept a greater-than-equal operation" in {
      parseSingle("1 >= 2") must beLike { case GtEq(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a greater-than-equal operation lacking a left operand" in {
      parseSingle(">= 2") must throwA[ParseException]
    }
    
    "reject a greater-than-equal operation lacking a right operand" in {
      parseSingle("1 >=") must throwA[ParseException]
    }
    
    "accept a equality operation" in {
      parseSingle("1 = 2") must beLike { case Eq(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject an equality operation lacking a left operand" in {
      parseSingle("= 2") must throwA[ParseException]
    }
    
    "reject an equality operation lacking a right operand" in {
      parseSingle("1 =") must throwA[ParseException]
    }
    
    "accept a not-equal operation" in {
      parseSingle("1 != 2") must beLike { case NotEq(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a not-equal operation lacking a left operand" in {
      parseSingle("!= 2") must throwA[ParseException]
    }
    
    "reject a not-equal operation lacking a right operand" in {
      parseSingle("1 !=") must throwA[ParseException]
    }
    
    "accept a boolean and operation" in {
      parseSingle("1 & 2") must beLike { case And(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a boolean and operation lacking a left operand" in {
      parseSingle("& 2") must throwA[ParseException]
    }
    
    "reject a boolean and operation lacking a right operand" in {
      parseSingle("1 &") must throwA[ParseException]
    }
    
    "accept a boolean or operation" in {
      parseSingle("1 | 2") must beLike { case Or(_, NumLit(_, "1"), NumLit(_, "2")) => ok }
    }
    
    "reject a boolean or operation lacking a left operand" in {
      parseSingle("| 2") must throwA[ParseException]
    }
    
    "reject a boolean or operation lacking a right operand" in {
      parseSingle("1 |") must throwA[ParseException]
    }
    
    "accept boolean complementation" in {
      parseSingle("!1") must beLike { case Comp(_, NumLit(_, "1")) => ok }
    }
    
    "accept numeric negation" in {
      parseSingle("neg 1") must beLike { case Neg(_, NumLit(_, "1")) => ok }
    }
    
    "accept parentheticals" in {
      parseSingle("(1)") must beLike { case Paren(_, NumLit(_, "1")) => ok }
    }
    
    "reject unmatched parentheses" in {
      parseSingle("(") must throwA[ParseException]
      parseSingle("(()") must throwA[ParseException]
    }
  }
  
  "operator precedence" should {
    "favor descent/deref over negation/complement" in {
      parseSingle("!1.x") must beLike {
        case Comp(_, Descent(_, NumLit(_, "1"), "x")) => ok
      }
      
      parseSingle("neg 1.x") must beLike {
        case Neg(_, Descent(_, NumLit(_, "1"), "x")) => ok
      }
      
      parseSingle("!1@x") must beLike {
        case Comp(_, MetaDescent(_, NumLit(_, "1"), "x")) => ok
      }
      
      parseSingle("neg 1@x") must beLike {
        case Neg(_, MetaDescent(_, NumLit(_, "1"), "x")) => ok
      }
      
      parseSingle("!1[2]") must beLike {
        case Comp(_, Deref(_, NumLit(_, "1"), NumLit(_, "2"))) => ok
      }
      
      parseSingle("neg 1[2]") must beLike {
        case Neg(_, Deref(_, NumLit(_, "1"), NumLit(_, "2"))) => ok
      }
    }
    
    "favor negation/complement over multiplication/division" in {
      parseSingle("!a * b") must beLike { case Mul(_, Comp(_, Dispatch(_, Identifier(Vector(), "a"), Vector())), Dispatch(_, Identifier(Vector(), "b"), Vector())) => ok }
      parseSingle("neg a * b") must beLike { case Mul(_, Neg(_, Dispatch(_, Identifier(Vector(), "a"), Vector())), Dispatch(_, Identifier(Vector(), "b"), Vector())) => ok }
      parseSingle("!a / b") must beLike { case Div(_, Comp(_, Dispatch(_, Identifier(Vector(), "a"), Vector())), Dispatch(_, Identifier(Vector(), "b"), Vector())) => ok }
      parseSingle("neg a / b") must beLike { case Div(_, Neg(_, Dispatch(_, Identifier(Vector(), "a"), Vector())), Dispatch(_, Identifier(Vector(), "b"), Vector())) => ok }
      parseSingle("!a % b") must beLike { case Mod(_, Comp(_, Dispatch(_, Identifier(Vector(), "a"), Vector())), Dispatch(_, Identifier(Vector(), "b"), Vector())) => ok }
      parseSingle("neg a % b") must beLike { case Mod(_, Neg(_, Dispatch(_, Identifier(Vector(), "a"), Vector())), Dispatch(_, Identifier(Vector(), "b"), Vector())) => ok }
    }
    
    "favor multiplication/division over addition/subtraction" in {
      parseSingle("a + b * c") must beLike { case Add(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Mul(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a - b * c") must beLike { case Sub(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Mul(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a * b + c") must beLike { case Add(_, Mul(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      parseSingle("a * b - c") must beLike { case Sub(_, Mul(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      
      parseSingle("a + b / c") must beLike { case Add(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Div(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a - b / c") must beLike { case Sub(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Div(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a / b + c") must beLike { case Add(_, Div(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      parseSingle("a / b - c") must beLike { case Sub(_, Div(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      
      parseSingle("a + b % c") must beLike { case Add(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Mod(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a - b % c") must beLike { case Sub(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Mod(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a % b + c") must beLike { case Add(_, Mod(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      parseSingle("a % b - c") must beLike { case Sub(_, Mod(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
    }
    
    "favor multiplication/division according to left/right ordering" in {
      parseSingle("1 * 2 / 3") must beLike { case Div(_, Mul(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
      parseSingle("1 / 2 * 3") must beLike { case Mul(_, Div(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
      
      parseSingle("1 * 2 % 3") must beLike { case Mod(_, Mul(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
      parseSingle("1 % 2 * 3") must beLike { case Mul(_, Mod(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
      
      parseSingle("1 % 2 / 3") must beLike { case Div(_, Mod(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
      parseSingle("1 / 2 % 3") must beLike { case Mod(_, Div(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
    }

    "favor power according to left/right ordering" in {
      parseSingle("1 / 2 ^ 3") must beLike { case Div(_, NumLit(_, "1"), Pow(_, NumLit(_, "2"), NumLit(_, "3"))) => ok }

      parseSingle("1 ^ 2 ^ 3") must beLike { case Pow(_, Pow(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
    }
    
    "favor addition/subtraction according to left/right ordering" in {
      parseSingle("1 + 2 - 3") must beLike { case Sub(_, Add(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
      parseSingle("1 - 2 + 3") must beLike { case Add(_, Sub(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
    }
    
    "favor addition/subtraction over inequality operators" in {
      parseSingle("a < b + c") must beLike { case Lt(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Add(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a <= b + c") must beLike { case LtEq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Add(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a + b < c") must beLike { case Lt(_, Add(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      parseSingle("a + b <= c") must beLike { case LtEq(_, Add(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      
      parseSingle("a < b - c") must beLike { case Lt(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Sub(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a <= b - c") must beLike { case LtEq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Sub(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a - b < c") must beLike { case Lt(_, Sub(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      parseSingle("a - b <= c") must beLike { case LtEq(_, Sub(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
                                                     
      parseSingle("a > b + c") must beLike { case Gt(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Add(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a >= b + c") must beLike { case GtEq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Add(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a + b > c") must beLike { case Gt(_, Add(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      parseSingle("a + b >= c") must beLike { case GtEq(_, Add(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      
      parseSingle("a > b - c") must beLike { case Gt(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Sub(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a >= b - c") must beLike { case GtEq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Sub(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a - b > c") must beLike { case Gt(_, Sub(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      parseSingle("a - b >= c") must beLike { case GtEq(_, Sub(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
    }
    
    "favor inequality operators according to left/right ordering" in {
      parseSingle("1 < 2 <= 3") must beLike { case LtEq(_, Lt(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
      parseSingle("1 < 2 > 3") must beLike { case Gt(_, Lt(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
      parseSingle("1 < 2 >= 3") must beLike { case GtEq(_, Lt(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
      
      parseSingle("1 <= 2 < 3") must beLike { case Lt(_, LtEq(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
      parseSingle("1 > 2 < 3") must beLike { case Lt(_, Gt(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
      parseSingle("1 >= 2 < 3") must beLike { case Lt(_, GtEq(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
      
      // note: doesn't actually test *every* case, but it's hard to imagine someone introducing a bug here
    }
    
    "favor inequality operators over equality operators" in {
      parseSingle("a = b < c") must beLike { case Eq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Lt(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a != b < c") must beLike { case NotEq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Lt(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a < b = c") must beLike { case Eq(_, Lt(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      parseSingle("a < b != c") must beLike { case NotEq(_, Lt(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      
      parseSingle("a = b <= c") must beLike { case Eq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), LtEq(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a != b <= c") must beLike { case NotEq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), LtEq(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a <= b = c") must beLike { case Eq(_, LtEq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      parseSingle("a <= b != c") must beLike { case NotEq(_, LtEq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      
      parseSingle("a = b > c") must beLike { case Eq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Gt(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a != b > c") must beLike { case NotEq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Gt(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a > b = c") must beLike { case Eq(_, Gt(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      parseSingle("a > b != c") must beLike { case NotEq(_, Gt(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      
      parseSingle("a = b >= c") must beLike { case Eq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), GtEq(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a != b >= c") must beLike { case NotEq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), GtEq(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a >= b = c") must beLike { case Eq(_, GtEq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      parseSingle("a >= b != c") must beLike { case NotEq(_, GtEq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
    }
    
    "favor equality operators according to left/right ordering" in {
      parseSingle("1 = 2 != 3") must beLike { case NotEq(_, Eq(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
      parseSingle("1 != 2 = 3") must beLike { case Eq(_, NotEq(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
    }
    
    "favor equality operators over and/or" in {
      parseSingle("a & b = c") must beLike { case And(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Eq(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a | b = c") must beLike { case Or(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Eq(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a = b & c") must beLike { case And(_, Eq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      parseSingle("a = b | c") must beLike { case Or(_, Eq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      
      parseSingle("a & b != c") must beLike { case And(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), NotEq(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a | b != c") must beLike { case Or(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), NotEq(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a != b & c") must beLike { case And(_, NotEq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      parseSingle("a != b | c") must beLike { case Or(_, NotEq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
    }
    
    "favor and/or according to left/right ordering" in {
      parseSingle("1 & 2 | 3") must beLike { case Or(_, And(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
      parseSingle("1 | 2 & 3") must beLike { case And(_, Or(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
    }
    
    "favor and/or operators over union/intersect/diff" in {
      parseSingle("a union b & c") must beLike { case Union(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), And(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a intersect b & c") must beLike { case Intersect(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), And(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a difference b & c") must beLike { case Difference(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), And(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a & b union c") must beLike { case Union(_, And(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      parseSingle("a & b intersect c") must beLike { case Intersect(_, And(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      parseSingle("a & b difference c") must beLike { case Difference(_, And(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      
      parseSingle("a union b | c") must beLike { case Union(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Or(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a intersect b | c") must beLike { case Intersect(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Or(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a difference b | c") must beLike { case Difference(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Or(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a | b union c") must beLike { case Union(_, Or(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      parseSingle("a | b intersect c") must beLike { case Intersect(_, Or(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      parseSingle("a | b difference c") must beLike { case Difference(_, Or(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
    }
    
    "favor union/intersect/diff according to left/right ordering" in {
      parseSingle("1 union 2 intersect 3") must beLike { case Intersect(_, Union(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
      parseSingle("1 intersect 2 union 3") must beLike { case Union(_, Intersect(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
      
      parseSingle("1 union 2 difference 3") must beLike { case Difference(_, Union(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
      parseSingle("1 difference 2 union 3") must beLike { case Union(_, Difference(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")) => ok }
    }
    
    "favor union/intersect/diff over with" in {
      parseSingle("a with b union c") must beLike { case With(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Union(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a with b intersect c") must beLike { case With(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Intersect(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a with b difference c") must beLike { case With(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Difference(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()))) => ok }
      parseSingle("a union b with c") must beLike { case With(_, Union(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      parseSingle("a intersect b with c") must beLike { case With(_, Intersect(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
      parseSingle("a difference b with c") must beLike { case With(_, Difference(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
    }
    
    "favor with over new" in {
      parseSingle("new a with b") must beLike { case New(_, With(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector()))) => ok }
    }
    
    "favor new over where" in {
      parseSingle("new a where b") must beLike { case Where(_, New(_, Dispatch(_, Identifier(Vector(), "a"), Vector())), Dispatch(_, Identifier(Vector(), "b"), Vector())) => ok }
    }
    
    "favor where over relate" in {
      parseSingle("a where b ~ c d") must beLike { case Relate(_, Where(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector()), Dispatch(_, Identifier(Vector(), "d"), Vector())) => ok }
      parseSingle("a ~ b where c d") must beLike { case Relate(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Where(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector())), Dispatch(_, Identifier(Vector(), "d"), Vector())) => ok }
    }
    
    "favor where over let" in {
      parseSingle("a := 1 2 where 3") must beLike { case Let(_, Identifier(Vector(), "a"), Vector(), NumLit(_, "1"), Where(_, NumLit(_, "2"), NumLit(_, "3"))) => ok }
    }
    
    "favor where over assert" in {
      parseSingle("assert 1 2 where 3") must beLike { case Assert(_, NumLit(_, "1"), Where(_, NumLit(_, "2"), NumLit(_, "3"))) => ok }
    }
    
    "favor where over solve" in {
      parseSingle("solve 'a 2 where 3") must beLike { case Solve(_, Vector(TicVar(_, "'a")), Where(_, NumLit(_, "2"), NumLit(_, "3"))) => ok }
    }
    
    "favor where over import" in {
      parseSingle("import a 2 where 3") must beLike { case Import(_, SpecificImport(Vector("a")), Where(_, NumLit(_, "2"), NumLit(_, "3"))) => ok }
    }
  }
  
  "operator associativity" should {
    "associate relations to the right" in {
      parseSingle("a ~ b a ~ b 42") must beLike {
        case Relate(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector()),
               Relate(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector()),
                 NumLit(_, "42"))) => ok
      }
    }
    
    "associate multiplication to the left" in {
      parseSingle("a * b * c") must beLike { case Mul(_, Mul(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
    }
    
    "associate division to the left" in {
      parseSingle("a / b / c") must beLike { case Div(_, Div(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
    }
    
    "associate addition to the left" in {
      parseSingle("a + b + c") must beLike { case Add(_, Add(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
    }
    
    "associate subtraction to the left" in {
      parseSingle("a - b - c") must beLike { case Sub(_, Sub(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
    }
    
    "associate less-than to the left" in {
      parseSingle("a < b < c") must beLike { case Lt(_, Lt(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
    }
    
    "associate less-than-equal to the left" in {
      parseSingle("a <= b <= c") must beLike { case LtEq(_, LtEq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
    }
    
    "associate greater-than to the left" in {
      parseSingle("a > b > c") must beLike { case Gt(_, Gt(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
    }
    
    "associate greater-than-equal to the left" in {
      parseSingle("a >= b >= c") must beLike { case GtEq(_, GtEq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
    }
    
    "associate equal to the left" in {
      parseSingle("a = b = c") must beLike { case Eq(_, Eq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
    }
    
    "associate not-equal to the left" in {
      parseSingle("a != b != c") must beLike { case NotEq(_, NotEq(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
    }
    
    "associate where to the left" in {
      parseSingle("a where b where c") must beLike { case Where(_, Where(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector())), Dispatch(_, Identifier(Vector(), "c"), Vector())) => ok }
    }
    
    "apply within the body of a let" in {
      parseSingle("a := 1 + 2 + 3 4") must beLike {
        case Let(_, Identifier(Vector(), "a"), _, 
          Add(_, Add(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3")), NumLit(_, "4")) => ok
      }
    }
    
    "apply within the scope of a let" in {
      parseSingle("a := 4 1 + 2 + 3") must beLike {
        case Let(_, Identifier(Vector(), "a"), _, 
          NumLit(_, "4"), Add(_, Add(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "3"))) => ok
      }
    }
  }
  
  "whitespace processing" should {
    "skip any amount of leading space characters" in {
      parseSingle(" 1") must beLike { case NumLit(_, "1") => ok }
      parseSingle("     1") must beLike { case NumLit(_, "1") => ok }
      parseSingle("\t  \t 1") must beLike { case NumLit(_, "1") => ok }
      parseSingle("\n\r  ;\r\t  \t\n ;;1") must beLike { case NumLit(_, "1") => ok }
    }
    
    "skip any amount of trailing space characters" in {
      parseSingle("1 ") must beLike { case NumLit(_, "1") => ok }
      parseSingle("1     ") must beLike { case NumLit(_, "1") => ok }
      parseSingle("1\t  \t ") must beLike { case NumLit(_, "1") => ok }
      parseSingle("1\n\r  ;\r\t  \t\n ;;") must beLike { case NumLit(_, "1") => ok }
    }
    
    "skip leading line comments delimited by newline" in {
      parseSingle("-- testing one two three\n1") must beLike { case NumLit(_, "1") => ok }
      parseSingle("-- testing one two three\n--four five six\n1") must beLike { case NumLit(_, "1") => ok }
      parseSingle("   \t  -- testing one two three\n\n  \t--four five six\n\r  1") must beLike {
        case NumLit(_, "1") => ok
      }
    }
    
    "skip trailing line comments delimited by newline" in {
      parseSingle("1-- testing one two three\n") must beLike { case NumLit(_, "1") => ok }
      parseSingle("1-- testing one two three\n--four five six\n") must beLike { case NumLit(_, "1") => ok }
      parseSingle("1   \t  -- testing one two three\n\n  \t--four five six\n\r  ") must beLike {
        case NumLit(_, "1") => ok
      }
    }
    
    "skip leading block comments" in {
      parseSingle("(- testing one two three -)1") must beLike { case NumLit(_, "1") => ok }
      parseSingle("  (- testing one two three -)   1") must beLike { case NumLit(_, "1") => ok }
      parseSingle("  (- testing one \n two\n three -)   1") must beLike { case NumLit(_, "1") => ok }
      
      parseSingle("(- testing one two three -)\n(-four five six -)\n1") must beLike {
        case NumLit(_, "1") => ok
      }
      
      parseSingle("(- testing one- \\- two three -)\n(-four \n five-- \t six -)\n1") must beLike {
        case NumLit(_, "1") => ok
      }
      
      parseSingle("   \t  (- testing one two three -)\n\n  \t(- four five six -)\n\r  1") must beLike {
        case NumLit(_, "1") => ok
      }
    }
    
    "skip trailing line comments delimited by newline" in {
      parseSingle("1(- testing one two three -)") must beLike { case NumLit(_, "1") => ok }
      parseSingle("1  (- testing one two three -)    ") must beLike { case NumLit(_, "1") => ok }
      parseSingle("1  (- testing one \n two\n three -)  ") must beLike { case NumLit(_, "1") => ok }
      
      parseSingle("1\t  (- testing one two three -)\n(-four five six -)\n") must beLike {
        case NumLit(_, "1") => ok
      }
      
      parseSingle("1(- testing one- \\- two three -)\n(-four \n five-- \t six -)\n") must beLike {
        case NumLit(_, "1") => ok
      }
      
      parseSingle("1   \t  (- testing one two three -)\n\n  \t(- four five six -)\n\r  ") must beLike {
        case NumLit(_, "1") => ok
      }
    }
    
    // Regression test for #39825209
    "ambiguous comment syntax" in {
      parseSingle("(-- Test\n--) 1") must beLike {
        case NumLit(_, "1") => ok
      }
    }

    "greedily terminate comment blocks" in {
      parseSingle("(-a--)-)42") must throwA[ParseException]
    }
  }
  
  "composed expression parsing" should {
    "parseSingle a no param function containing a parenthetical" in {
      parseSingle("a := 1 (2)") must beLike {
        case Let(_, Identifier(Vector(), "a"), Vector(), NumLit(_, "1"), Paren(_, NumLit(_, "2"))) => ok
      }
    }
    
    "parseSingle a no param function containing a no param function" in {
      parseSingle("a := 1 c := 2 3") must beLike {
        case Let(_, Identifier(Vector(), "a"), Vector(), NumLit(_, "1"), Let(_, Identifier(Vector(), "c"), Vector(), NumLit(_, "2"), NumLit(_, "3"))) => ok
      }
    }
    
    "parseSingle a no param function containing a 1 param function" in {
      parseSingle("a := 1 c(d) := 2 3") must beLike {
        case Let(_, Identifier(Vector(), "a"), Vector(), NumLit(_, "1"), Let(_, Identifier(Vector(), "c"), Vector("d"), NumLit(_, "2"), NumLit(_, "3"))) => ok
      }
    }
    
    "correctly nest multiple binds" in {
      val input = """
        | a :=
        |   b := //f
        |   c := //g
        |
        |   d
        | e""".stripMargin
      
      parseSingle(input) must beLike {
        case Let(_, Identifier(Vector(), "a"), Vector(), Let(_, Identifier(Vector(), "b"), Vector(), PathLit("/f"), Let(_, Identifier(Vector(), "c"), Vector(), PathLit("/g"), Dispatch(_, Identifier(Vector(), "d"), Vector()))), Dispatch(_, Identifier(Vector(), "e"), Vector())) => ok
      }
    }
    
    "handle new expression within deref parameter" in {
      parseSingle("1[new 2]") must beLike {
        case Deref(_, NumLit(_, "1"), New(_, NumLit(_, "2"))) => ok
      }
    }
    
    "accept a binary expression involving a number and a string" in {
      parseSingle("1 + \"a\"") must beLike {
        case Add(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parseSingle("1 - \"a\"") must beLike {
        case Sub(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parseSingle("1 * \"a\"") must beLike {
        case Mul(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parseSingle("1 / \"a\"") must beLike {
        case Div(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parseSingle("1 < \"a\"") must beLike {
        case Lt(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parseSingle("1 <= \"a\"") must beLike {
        case LtEq(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parseSingle("1 > \"a\"") must beLike {
        case Gt(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parseSingle("1 >= \"a\"") must beLike {
        case GtEq(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parseSingle("1 = \"a\"") must beLike {
        case Eq(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parseSingle("1 != \"a\"") must beLike {
        case NotEq(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parseSingle("1 & \"a\"") must beLike {
        case And(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parseSingle("1 | \"a\"") must beLike {
        case Or(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parseSingle("1 where \"a\"") must beLike {
        case Where(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parseSingle("1 with \"a\"") must beLike {
        case With(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parseSingle("1 union \"a\"") must beLike {
        case Union(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
      
      parseSingle("1 intersect \"a\"") must beLike {
        case Intersect(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }      

      parseSingle("1 difference \"a\"") must beLike {
        case Difference(_, NumLit(_, "1"), StrLit(_, "a")) => ok
      }
    }
  }
  
  "global ambiguity resolution" should {
    "recognize <keyword>foo as an identifier" in {
      forall(keywords) { kw =>
        val input = kw + "foo"
        parseSingle(input) must beLike {
          case Dispatch(_, Identifier(Vector(), `input`), Vector()) => ok
        }
      }
    }
    
    "reject squashed where expression" in {
      parseSingle("a whereb") must throwA[ParseException]
    }
    
    "reject squashed with expression" in {
      parseSingle("a withb") must throwA[ParseException]
    }
    
    "associate paired consecutive parentheses" in {
      parseSingle("a := b := c (d) (e)") must beLike {
        case Let(_, Identifier(Vector(), "a"), Vector(), Let(_, Identifier(Vector(), "b"), Vector(), Dispatch(_, Identifier(Vector(), "c"), Vector()), Paren(_, Dispatch(_, Identifier(Vector(), "d"), Vector()))), Paren(_, Dispatch(_, Identifier(Vector(), "e"), Vector()))) => ok
      }
    }
    
    "disambiguate one-argument function within n-ary relation" in {
      val input = """
        | a ~
        |   b ~ c
        |     d := f
        |     (1)
        |   2""".stripMargin
        
      parseSingle(input) must beLike {
        case Relate(_, Dispatch(_, Identifier(Vector(), "a"), Vector()), Dispatch(_, Identifier(Vector(), "b"), Vector()),
          Relate(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector()),
            Let(_, Identifier(Vector(), "d"), Vector(), Dispatch(_, Identifier(Vector(), "f"), Vector(NumLit(_, "1"))), NumLit(_, "2")))) => ok
      }
    }
    
    "consume string literals non-greedily" >> {
      "valid" >> {
        parseSingle("""{ user: "daniel", last: "spiewak" }""") must beLike {
          case ObjectDef(_, Vector(("user", StrLit(_, "daniel")), ("last", StrLit(_, "spiewak")))) => ok
        }
      }
      
      "invalid" >> {
        parseSingle("""{ user: "daniel", igly boio" }""") must throwA[ParseException]
      }
    }
    
    "correctly disambiguate chained array dereferences" in {
      parseSingle("a := b [c] [d]") must beLike {
        case Let(_, Identifier(Vector(), "a"), Vector(),
          Deref(_, Dispatch(_, Identifier(Vector(), "b"), Vector()), Dispatch(_, Identifier(Vector(), "c"), Vector())),
          ArrayDef(_, Vector(Dispatch(_, Identifier(Vector(), "d"), Vector())))) => ok
      }
    }
    
    "produce all valid trees in case of legitimate ambiguity" in {
      val forest = parse("a := c (42) [1]")
      forest must haveSize(2)
      
      {
        forest.head must beLike {
          case Let(_, Identifier(Vector(), "a"), Vector(),
            Dispatch(_, Identifier(Vector(), "c"), Vector()),
            Deref(_, 
              Paren(_, NumLit(_, "42")),
              NumLit(_, "1"))) => ok
        }
        
        forest.last must beLike {
          case Let(_, Identifier(Vector(), "a"), Vector(),
            Dispatch(_, Identifier(Vector(), "c"), Vector(NumLit(_, "42"))),
            ArrayDef(_, Vector(NumLit(_, "1")))) => ok
        }
      } or {
        forest.head must beLike {
          case Let(_, Identifier(Vector(), "a"), Vector(),
            Dispatch(_, Identifier(Vector(), "c"), Vector(NumLit(_, "42"))),
            ArrayDef(_, Vector(NumLit(_, "1")))) => ok
        }
        
        forest.last must beLike {
          case Let(_, Identifier(Vector(), "a"), Vector(),
            Dispatch(_, Identifier(Vector(), "c"), Vector()),
            Deref(_, 
              Paren(_, NumLit(_, "42")),
              NumLit(_, "1"))) => ok
        }
      }
    }
    
    "prefer path literals in case of ambiguity" in {
      parseSingle("//foo.bar") must beLike {
        case PathLit("/foo.bar") => ok
      }
    }
  }

  val exampleDir = new File("quirrel/examples")
  
  if (exampleDir.exists) {
    "specification examples" >> {
      for (file <- exampleDir.listFiles if file.getName endsWith ".qrl") {
        file.getName >> {
          parseSingle(LineStream(Source.fromFile(file))) must not(throwA[Throwable])
        }
      }
    }
  } else {
    "specification examples" >> skipped
  }
  
 
  private def parseSingle(str: LineStream): Expr = {
    val set = parse(str)
    set must haveSize(1)
    set.head
  }
  
  private def parseSingle(str: String): Expr = parseSingle(LineStream(str))
  
  private object PathLit {
    def unapply(expr: Expr): Option[String] = expr match {
      case Dispatch(_, Identifier(Vector(), "load"), Vector(
        Dispatch(_, Identifier(Vector("std", "fs"), "expandPath"), Vector(
          StrLit(_, str))))) => Some(str)
        
      case _ => None
    }
  }
  
  private object RelPathLit {
    def unapply(expr: Expr): Option[String] = expr match {
      case Dispatch(_, Identifier(Vector(), "relativeLoad"), Vector(
        Dispatch(_, Identifier(Vector("std", "fs"), "expandPath"), Vector(
          StrLit(_, str))))) => Some(str)
        
      case _ => None
    }
  }
}
