package com.querio.quirrel

import edu.uwm.cs.gll.LineStream
import edu.uwm.cs.gll.ast.Node

import org.specs2.mutable.Specification

object SolverSpecs extends Specification with parser.Parser with Solver with StubPhases {
  import ast._
  
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
    
    "solve parenthetical" in {
      solve("('a)", 'a) must beLike {
        case Some(NumLit(_, "0")) => ok
      }
    }
  }
  
  "compound binary expression solution" should {
    "solve chained addition and multiplication" in {
      solve("'a * 2 + 3", 'a) must beLike {
        case Some(Div(_, Sub(_, NumLit(_, "0"), NumLit(_, "3")), NumLit(_, "2"))) => ok
      }
      
      solve("3 + 2 * 'a", 'a) must beLike {
        case Some(Div(_, Sub(_, NumLit(_, "0"), NumLit(_, "3")), NumLit(_, "2"))) => ok
      }
    }
    
    "solve paired variable addition" in {
      solve("'a + 'a", 'a) must beLike {
        case Some(Div(_, NumLit(_, "0"), NumLit(_, "2"))) => ok
      }
    }
    
    "fail to solve paired variable subtraction" in {
      solve("'a - 'a", 'a) must beLike {
        case None => ok
      }
    }
    
    "fail to solve paired variable multiplication" in {
      solve("'a * 'a", 'a) must beLike {
        case None => ok
      }
    }
    
    "fail to solve paired variable division" in {
      solve("'a / 'a", 'a) must beLike {
        case None => ok
      }
    }
    
    "solve addition across multiplicand" in {
      solve("'a * 2 + 'a", 'a) must beLike {
        case Some(Div(_, NumLit(_, "0"), NumLit(_, "3"))) => ok
        case Some(Div(_, NumLit(_, "0"), Add(_, NumLit(_, "2"), NumLit(_, "1")))) => ok
      }
      
      solve("'a + 'a * 2", 'a) must beLike {
        case Some(Div(_, NumLit(_, "0"), NumLit(_, "3"))) => ok
        case Some(Div(_, NumLit(_, "0"), Add(_, NumLit(_, "2"), NumLit(_, "1")))) => ok
      }
    }
    
    "solve multi-addition across multiplicand" in {
      solve("'a * 2 + 'a + 'a", 'a) must beLike {
        case Some(Div(_, NumLit(_, "0"), NumLit(_, "4"))) => ok
        case Some(Div(_, NumLit(_, "0"), Add(_, Add(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "1")))) => ok
        case Some(Div(_, NumLit(_, "0"), Add(_, Add(_, NumLit(_, "2"), NumLit(_, "1")), NumLit(_, "1")))) => ok
        case Some(Div(_, Div(_, NumLit(_, "0"), NumLit(_, "2")), NumLit(_, "2"))) => ok
        case Some(Div(_, NumLit(_, "0"), Add(_, NumLit(_, "2"), NumLit(_, "2")))) => ok
      }
      
      solve("'a + 'a * 2 + 'a", 'a) must beLike {
        case Some(Div(_, NumLit(_, "0"), NumLit(_, "4"))) => ok
        case Some(Div(_, NumLit(_, "0"), Add(_, Add(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "1")))) => ok
        case Some(Div(_, NumLit(_, "0"), Add(_, Add(_, NumLit(_, "2"), NumLit(_, "1")), NumLit(_, "1")))) => ok
        case Some(Div(_, Div(_, NumLit(_, "0"), NumLit(_, "2")), NumLit(_, "2"))) => ok
        case Some(Div(_, NumLit(_, "0"), Add(_, NumLit(_, "2"), NumLit(_, "2")))) => ok
      }
      
      solve("'a + 'a + 'a * 2", 'a) must beLike {
        case Some(Div(_, NumLit(_, "0"), NumLit(_, "4"))) => ok
        case Some(Div(_, NumLit(_, "0"), Add(_, Add(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "1")))) => ok
        case Some(Div(_, NumLit(_, "0"), Add(_, Add(_, NumLit(_, "2"), NumLit(_, "1")), NumLit(_, "1")))) => ok
        case Some(Div(_, Div(_, NumLit(_, "0"), NumLit(_, "2")), NumLit(_, "2"))) => ok
        case Some(Div(_, NumLit(_, "0"), Add(_, NumLit(_, "2"), NumLit(_, "2")))) => ok
      }
    }
    
    "solve multi-addition parenthetical across multiplicand" in {
      solve("'a * 2 + ('a + 'a)", 'a) must beLike {
        case Some(Div(_, NumLit(_, "0"), NumLit(_, "4"))) => ok
        case Some(Div(_, Div(_, NumLit(_, "0"), NumLit(_, "2")), NumLit(_, "2"))) => ok
        case Some(Div(_, NumLit(_, "0"), Add(_, NumLit(_, "2"), NumLit(_, "2")))) => ok
        case Some(Div(_, NumLit(_, "0"), Add(_, Add(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "1")))) => ok
        case Some(Div(_, NumLit(_, "0"), Add(_, Add(_, NumLit(_, "2"), NumLit(_, "1")), NumLit(_, "1")))) => ok
      }
      
      solve("('a + 'a) + 'a * 2", 'a) must beLike {
        case Some(Div(_, NumLit(_, "0"), NumLit(_, "4"))) => ok
        case Some(Div(_, Div(_, NumLit(_, "0"), NumLit(_, "2")), NumLit(_, "2"))) => ok
        case Some(Div(_, NumLit(_, "0"), Add(_, NumLit(_, "2"), NumLit(_, "2")))) => ok
        case Some(Div(_, NumLit(_, "0"), Add(_, Add(_, NumLit(_, "1"), NumLit(_, "2")), NumLit(_, "1")))) => ok
        case Some(Div(_, NumLit(_, "0"), Add(_, Add(_, NumLit(_, "2"), NumLit(_, "1")), NumLit(_, "1")))) => ok
      }
    }
    
    "solve addition across dividend" in {
      solve("'a / 2 + 'a", 'a) must beLike {
        case Some(Div(_, Mul(_, NumLit(_, "0"), NumLit(_, "2")), NumLit(_, "3"))) => ok
        case Some(Div(_, Mul(_, NumLit(_, "0"), NumLit(_, "2")), Add(_, NumLit(_, "2"), NumLit(_, "1")))) => ok
      }
      
      solve("'a + 'a / 2", 'a) must beLike {
        case Some(Div(_, Mul(_, NumLit(_, "0"), NumLit(_, "2")), NumLit(_, "3"))) => ok
        case Some(Div(_, Mul(_, NumLit(_, "0"), NumLit(_, "2")), Add(_, NumLit(_, "2"), NumLit(_, "1")))) => ok
      }
    }
    
    "solve addition of multiplicands with common factors" in {
      solve("2 * 'a + 3 * 'a", 'a) must beLike {
        case Some(Div(_, NumLit(_, "0"), Add(_, NumLit(_, "2"), NumLit(_, "3")))) => ok
        case Some(Div(_, NumLit(_, "0"), Add(_, NumLit(_, "3"), NumLit(_, "2")))) => ok
      }
      
      solve("3 * 'a + 2 * 'a", 'a) must beLike {
        case Some(Div(_, NumLit(_, "0"), Add(_, NumLit(_, "2"), NumLit(_, "3")))) => ok
        case Some(Div(_, NumLit(_, "0"), Add(_, NumLit(_, "3"), NumLit(_, "2")))) => ok
      }
    }
    
    "solve addition of dividends" in {
      solve("'a / 2 + 'a / 3", 'a) must beLike {
        case Some(Div(_, Mul(_, NumLit(_, "0"), Mul(_, NumLit(_, "2"), NumLit(_, "3"))), Add(_, NumLit(_, "3"), NumLit(_, "2")))) => ok
        case Some(Div(_, Mul(_, NumLit(_, "0"), Mul(_, NumLit(_, "3"), NumLit(_, "2"))), Add(_, NumLit(_, "2"), NumLit(_, "3")))) => ok
        case Some(Div(_, Mul(_, Mul(_, NumLit(_, "0"), NumLit(_, "2")), NumLit(_, "3")), Add(_, NumLit(_, "2"), NumLit(_, "3")))) => ok
        case Some(Div(_, Mul(_, Mul(_, NumLit(_, "0"), NumLit(_, "3")), NumLit(_, "2")), Add(_, NumLit(_, "3"), NumLit(_, "2")))) => ok
      }
      
      solve("'a / 3 + 'a / 2", 'a) must beLike {
        case Some(Div(_, Mul(_, NumLit(_, "0"), Mul(_, NumLit(_, "2"), NumLit(_, "3"))), Add(_, NumLit(_, "3"), NumLit(_, "2")))) => ok
        case Some(Div(_, Mul(_, NumLit(_, "0"), Mul(_, NumLit(_, "3"), NumLit(_, "2"))), Add(_, NumLit(_, "2"), NumLit(_, "3")))) => ok
        case Some(Div(_, Mul(_, Mul(_, NumLit(_, "0"), NumLit(_, "2")), NumLit(_, "3")), Add(_, NumLit(_, "2"), NumLit(_, "3")))) => ok
        case Some(Div(_, Mul(_, Mul(_, NumLit(_, "0"), NumLit(_, "3")), NumLit(_, "2")), Add(_, NumLit(_, "3"), NumLit(_, "2")))) => ok
      }
    }
    
    "solve subtraction of dividends" in {
      solve("('a * 3) / 2 - 'a / 3", 'a) must beLike {
        case Some(Div(_, Mul(_, NumLit(_, "0"), Mul(_, NumLit(_, "2"), NumLit(_, "3"))), Add(_, Mul(_, NumLit(_, "3"), NumLit(_, "3")), Neg(_, NumLit(_, "2"))))) => ok
        case Some(Div(_, Mul(_, NumLit(_, "0"), Mul(_, NumLit(_, "2"), Neg(_, NumLit(_, "3")))), Add(_, Mul(_, NumLit(_, "3"), Neg(_, NumLit(_, "3"))), NumLit(_, "2")))) => ok
        case Some(Div(_, Mul(_, NumLit(_, "0"), Mul(_, NumLit(_, "2"), Neg(_, NumLit(_, "3")))), Add(_, Mul(_, Neg(_, NumLit(_, "3")), NumLit(_, "3")), NumLit(_, "2")))) => ok
        case Some(Div(_, Mul(_, NumLit(_, "0"), Mul(_, NumLit(_, "2"), NumLit(_, "3"))), Sub(_, Mul(_, NumLit(_, "3"), NumLit(_, "3")), NumLit(_, "2")))) => ok
        case Some(Div(_, Mul(_, Mul(_, NumLit(_, "0"), NumLit(_, "2")), NumLit(_, "3")), Add(_, Neg(_, NumLit(_, "2")), Mul(_, NumLit(_, "3"), NumLit(_, "3"))))) => ok
        case Some(Div(_, Mul(_, Mul(_, NumLit(_, "0"), NumLit(_, "2")), Neg(_, NumLit(_, "3"))), Add(_, NumLit(_, "2"), Mul(_, NumLit(_, "3"), Neg(_, NumLit(_, "3")))))) => ok
      }
      
      solve("'a / 3 - ('a * 3) / 2", 'a) must beLike {
        case Some(Div(_, Mul(_, NumLit(_, "0"), Mul(_, NumLit(_, "2"), NumLit(_, "3"))), Sub(_, NumLit(_, "2"), Mul(_, NumLit(_, "3"), NumLit(_, "3"))))) => ok
        case Some(Div(_, Mul(_, NumLit(_, "0"), Mul(_, NumLit(_, "3"), NumLit(_, "2"))), Sub(_, NumLit(_, "2"), Mul(_, NumLit(_, "3"), NumLit(_, "3"))))) => ok
        case Some(Div(_, Mul(_, Mul(_, NumLit(_, "0"), NumLit(_, "3")), NumLit(_, "2")), Add(_, Mul(_, NumLit(_, "3"), Neg(_, NumLit(_, "3"))), NumLit(_, "2")))) => ok
        case Some(Div(_, Mul(_, Mul(_, NumLit(_, "0"), NumLit(_, "3")), Neg(_, NumLit(_, "2"))), Add(_, Mul(_, NumLit(_, "3"), NumLit(_, "3")), Neg(_, NumLit(_, "2"))))) => ok
        case Some(Div(_, Mul(_, Mul(_, NumLit(_, "0"), NumLit(_, "3")), NumLit(_, "2")), Sub(_, NumLit(_, "2"), Mul(_, NumLit(_, "3"), NumLit(_, "3"))))) => ok
        case Some(Div(_, Mul(_, Mul(_, NumLit(_, "0"), NumLit(_, "3")), NumLit(_, "2")), Add(_, Neg(_, Mul(_, NumLit(_, "3"), NumLit(_, "3"))), NumLit(_, "2")))) => ok
      }
    }
    
    "solve self multiplication of self dividend" in {
      solve("('a / 'a) * 'a", 'a) must beLike {
        case Some(NumLit(_, "0")) => ok
        case Some(Div(_, NumLit(_, "0"), NumLit(_, "1"))) => ok
      }
      
      solve("'a * ('a / 'a)", 'a) must beLike {
        case Some(NumLit(_, "0")) => ok
        case Some(Div(_, NumLit(_, "0"), NumLit(_, "1"))) => ok
      }
    }
    
    "solve self addition of self dividend" in {
      solve("('a / 'a) + 'a", 'a) must beLike {
        case Some(Sub(_, NumLit(_, "0"), NumLit(_, "1"))) => ok
      }
      
      solve("'a + ('a / 'a)", 'a) must beLike {
        case Some(Sub(_, NumLit(_, "0"), NumLit(_, "1"))) => ok
      }
    }
    
    "solve addition with negation" in {
      solve("2 * 'a + ~'a", 'a) must beLike {
        case Some(Div(_, NumLit(_, "0"), Sub(_, NumLit(_, "2"), NumLit(_, "1")))) => ok
      }
      
      solve("~'a + 2 * 'a", 'a) must beLike {
        case Some(Div(_, NumLit(_, "0"), Sub(_, NumLit(_, "2"), NumLit(_, "1")))) => ok
      }
    }
    
    "solve multiplication of dividends" in {
      solve("(2 / 'a) * ('a * 'a / 3)", 'a) must beLike {
        case Some(Div(_, Mul(_, NumLit(_, "0"), NumLit(_, "3")), NumLit(_, "2"))) => ok
      }
      
      solve("('a * 'a / 3) * (2 / 'a)", 'a) must beLike {
        case Some(Div(_, Mul(_, NumLit(_, "0"), NumLit(_, "3")), NumLit(_, "2"))) => ok
      }
    }
    
    "solve division of dividends" in {
      solve("(2 / 'a) / (3 / ('a * 'a))", 'a) must beLike {
        case Some(Div(_, Mul(_, NumLit(_, "0"), NumLit(_, "3")), NumLit(_, "2"))) => ok
      }
      
      solve("('a * 'a / 3) / ('a / 2)", 'a) must beLike {
        case Some(Div(_, Mul(_, NumLit(_, "0"), NumLit(_, "3")), NumLit(_, "2"))) => ok
      }
    }
  }

  "simple relation" should {
    "solve when target is in lhs of equality relation" in {
      solveRelation("(2 / 'a) / (3 / ('a * 'a)) = 0", 'a) must beLike {
        case Some(Div(_, Mul(_, NumLit(_, "0"), NumLit(_, "3")), NumLit(_, "2"))) => ok
      }
    }

    "solve when target is in rhs of equality relation" in {
      solveRelation("0 = (2 / 'a) / (3 / ('a * 'a))", 'a) must beLike {
        case Some(Div(_, Mul(_, NumLit(_, "0"), NumLit(_, "3")), NumLit(_, "2"))) => ok
      }
    }
  }
  
  def solve(str: String, id: Symbol): Option[Expr] = {
    val f = solve(parse(LineStream(str))) { case TicVar(_, id2) => id.toString == id2 }
    f(NumLit(LineStream(), "0"))
  }

  def solveRelation(str: String, id: Symbol): Option[Expr] = {
    val expr = parse(LineStream(str))

    solveRelation(expr.asInstanceOf[ExprBinaryNode]) { case TicVar(_, id2) => id.toString == id2; }
  }
}
