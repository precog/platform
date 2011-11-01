package com.reportgrid.quirrel

import edu.uwm.cs.gll.LineStream
import edu.uwm.cs.gll.ast.Node

import org.specs2.mutable.Specification

object SolverSpecs extends Specification with parser.Parser with Solver with StubPhases {
  
  "simple expression solution" should {
    "solve left addition" in {
      solve("'a + 2", 'a) must beLike {
        case Some(Sub(_, NumLit(_, "0"), NumLit(_, "2"))) => ok
      }
    }
    
    "solve right addition" in {
      solve("2 + 'a", 'a) must beLike {
        case Some(Sub(_, NumLit(_, "0"), NumLit(_, "2"))) => ok
      }
    }
    
    "solve left subtraction" in {
      solve("'a - 2", 'a) must beLike {
        case Some(Add(_, NumLit(_, "0"), NumLit(_, "2"))) => ok
      }
    }
    
    "solve right subtraction" in {
      solve("2 - 'a", 'a) must beLike {
        case Some(Neg(_, Sub(_, NumLit(_, "0"), NumLit(_, "2")))) => ok
      }
    }
    
    "solve left multiplication" in {
      solve("'a * 2", 'a) must beLike {
        case Some(Div(_, NumLit(_, "0"), NumLit(_, "2"))) => ok
      }
    }
    
    "solve right multiplication" in {
      solve("2 * 'a", 'a) must beLike {
        case Some(Div(_, NumLit(_, "0"), NumLit(_, "2"))) => ok
      }
    }
    
    "solve left division" in {
      solve("'a / 2", 'a) must beLike {
        case Some(Mul(_, NumLit(_, "0"), NumLit(_, "2"))) => ok
      }
    }
    
    "solve right division" in {
      solve("2 / 'a", 'a) must beLike {
        case Some(Div(_, NumLit(_, "2"), NumLit(_, "0"))) => ok
      }
    }
    
    "solve negation" in {
      solve("~'a", 'a) must beLike {
        case Some(Neg(_, NumLit(_, "0"))) => ok
      }
    }
  }
  
  def solve(str: String, id: Symbol): Option[Expr] = {
    val f = solve(parse(LineStream(str))) { case TicVar(_, id2) => id.toString == id2 }
    f(NumLit(LineStream(), "0"))
  }
}
