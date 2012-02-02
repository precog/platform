package com.precog
package daze

import com.precog.yggdrasil._
import org.specs2.mutable._

object EvaluatorSpecs extends Specification
    with Evaluator
    with OperationsAPI
    with DefaultYggConfig
    with StubQueryAPI {
      
  import Function._
  
  import dag._
  import instructions._
  
  "evaluator" should {
    "evaluate simple two-value multiplication" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(Mul),
        Root(line, PushNum("6")),
        Root(line, PushNum("7")))
        
      val result = consumeEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (Vector(), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(42)
    }
    
    "evaluate single value roots" >> {
      "push_string" >> {
        val line = Line(0, "")
        val input = Root(line, PushString("daniel"))
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SString(str)) => str
        }
        
        result2 must contain("daniel")
      }
      
      "push_num" >> {
        val line = Line(0, "")
        val input = Root(line, PushNum("42"))
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(42)
      }
      
      "push_true" >> {
        val line = Line(0, "")
        val input = Root(line, PushTrue)
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SBoolean(b)) => b
        }
        
        result2 must contain(true)
      }
      
      "push_false" >> {
        val line = Line(0, "")
        val input = Root(line, PushFalse)
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SBoolean(b)) => b
        }
        
        result2 must contain(false)
      }
      
      "push_object" >> {
        val line = Line(0, "")
        val input = Root(line, PushObject)
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SObject(obj)) => obj
        }
        
        result2 must contain(Map())
      }
      
      "push_array" >> {
        val line = Line(0, "")
        val input = Root(line, PushArray)
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SArray(arr)) => arr
        }
        
        result2 must contain(Vector())
      }
    }
    
    "evaluate a load_local" in {
      val line = Line(0, "")
      val input = dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het)
      val result = consumeEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (Vector(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(42, 12, 77, 1, 13)
    }
    
    "evaluate a negation mapped over numbers" in {
      val line = Line(0, "")
      
      val input = Operate(line, Neg,
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
        
      val result = consumeEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (Vector(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(-42, -12, -77, -1, -13)
    }
    
    "evaluate a new mapped over numbers as no-op" in {
      val line = Line(0, "")
      
      val input = dag.New(line,
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
        
      val result = consumeEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (Vector(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(42, 12, 77, 1, 13)
    }
    
    "evaluate a binary numeric operation mapped over homogeneous numeric set" >> {
      "addition" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(Add),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Root(line, PushNum("5")))
          
        val result = consumeEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(47, 17, 82, 6, 18)
      }
      
      "subtraction" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(Sub),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Root(line, PushNum("5")))
          
        val result = consumeEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(37, 7, 72, -4, 8)
      }
      
      "multiplication" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(Mul),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Root(line, PushNum("5")))
          
        val result = consumeEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(210, 60, 385, 5, 65)
      }
      
      "division" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(Div),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Root(line, PushNum("5")))
          
        val result = consumeEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toDouble
        }
        
        result2 must contain(8.4, 2.4, 15.4, 0.2, 2.6)
      }
    }
    
    "evaluate a binary numeric operation mapped over heterogeneous numeric set" >> {
      "addition" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(Add),
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Root(line, PushNum("5")))
          
        val result = consumeEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(47, 17, 82, 6, 18)
      }
      
      "subtraction" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(Sub),
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Root(line, PushNum("5")))
          
        val result = consumeEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(37, 7, 72, -4, 8)
      }
      
      "multiplication" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(Mul),
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Root(line, PushNum("5")))
          
        val result = consumeEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(210, 60, 385, 5, 65)
      }
      
      "division" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(Div),
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Root(line, PushNum("5")))
          
        val result = consumeEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toDouble
        }
        
        result2 must contain(8.4, 2.4, 15.4, 0.2, 2.6)
      }
    }
    
    "evaluate wrap_object on single values" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(WrapObject),
        Root(line, PushString("answer")),
        Root(line, PushNum("42")))
        
      val result = consumeEval(input)
      
      result must haveSize(1)
      
      val optObj = result find {
        case (Vector(), SObject(_)) => true
        case _ => false
      } collect {
        case (_, SObject(obj)) => obj
      }
      
      optObj must beSome
      val obj = optObj.get
      
      obj must haveKey("answer")
      obj("answer") must beLike {
        case SDecimal(d) => d mustEqual 42
      }
    }
    
    "evaluate wrap_array on a single value" in {
      val line = Line(0, "")
      
      val input = Operate(line, WrapArray,
        Root(line, PushNum("42")))
        
      val result = consumeEval(input)
      
      result must haveSize(1)
      
      val optArr = result find {
        case (Vector(), SArray(_)) => true
        case _ => false
      } collect {
        case (_, SArray(arr)) => arr
      }
      
      optArr must beSome
      val arr = optArr.get
      
      arr must haveSize(1)
      arr.head must beLike {
        case SDecimal(d) => d mustEqual 42
      }
    }
    
    "evaluate join_object on single values" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(JoinObject),
        Join(line, Map2Cross(WrapObject),
          Root(line, PushString("question")),
          Root(line, PushString("What is six times nine?"))),
        Join(line, Map2Cross(WrapObject),
          Root(line, PushString("answer")),
          Root(line, PushNum("42"))))
        
      val result = consumeEval(input)
      
      result must haveSize(1)
      
      val optObj = result find {
        case (Vector(), SObject(_)) => true
        case _ => false
      } collect {
        case (_, SObject(obj)) => obj
      }
      
      optObj must beSome
      val obj = optObj.get
      
      obj must haveKey("answer")
      obj("answer") must beLike {
        case SDecimal(d) => d mustEqual 42
      }
      
      obj must haveKey("question")
      obj("question") must beLike {
        case SString(str) => str mustEqual "What is six times nine?"
      }
    }
    
    "evaluate join_array on single values" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(JoinArray),
        Operate(line, WrapArray,
          Root(line, PushNum("24"))),
        Operate(line, WrapArray,
          Root(line, PushNum("42"))))
        
      val result = consumeEval(input)
      
      result must haveSize(1)
      
      val optArr = result find {
        case (Vector(), SArray(_)) => true
        case _ => false
      } collect {
        case (_, SArray(arr)) => arr
      }
      
      optArr must beSome
      val arr = optArr.get
      
      arr must beLike {
        case Vector(SDecimal(d1), SDecimal(d2)) => {
          d1 mustEqual 24
          d2 mustEqual 42
        }
      }
    }
    
    "evaluate array_swap on single values" >> {
      "at start" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(ArraySwap),
          Join(line, Map2Cross(JoinArray),
            Operate(line, WrapArray,
              Root(line, PushNum("12"))),
            Join(line, Map2Cross(JoinArray),
              Operate(line, WrapArray,
                Root(line, PushNum("24"))),
              Operate(line, WrapArray,
                Root(line, PushNum("42"))))),
          Root(line, PushNum("1")))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val optArr = result find {
          case (Vector(), SArray(_)) => true
          case _ => false
        } collect {
          case (_, SArray(arr)) => arr
        }
        
        optArr must beSome
        val arr = optArr.get
        
        arr must beLike {
          case Vector(SDecimal(d1), SDecimal(d2), SDecimal(d3)) => {
            d1 mustEqual 24
            d2 mustEqual 12
            d3 mustEqual 42
          }
        }
      }
      
      "at end" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(ArraySwap),
          Join(line, Map2Cross(JoinArray),
            Operate(line, WrapArray,
              Root(line, PushNum("12"))),
            Join(line, Map2Cross(JoinArray),
              Operate(line, WrapArray,
                Root(line, PushNum("24"))),
              Operate(line, WrapArray,
                Root(line, PushNum("42"))))),
          Root(line, PushNum("2")))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val optArr = result find {
          case (Vector(), SArray(_)) => true
          case _ => false
        } collect {
          case (_, SArray(arr)) => arr
        }
        
        optArr must beSome
        val arr = optArr.get
        
        arr must beLike {
          case Vector(SDecimal(d1), SDecimal(d2), SDecimal(d3)) => {
            d1 mustEqual 12
            d2 mustEqual 42
            d3 mustEqual 24
          }
        }
      }
    }
    
    "evaluate descent on a homogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(DerefObject),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/pairs")), Het),
        Root(line, PushString("first")))
        
      val result = consumeEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (Vector(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(42, 12, 77, 1, 13)
    }
    
    "evaluate descent on a heterogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(DerefObject),
        dag.LoadLocal(line, None, Root(line, PushString("/het/pairs")), Het),
        Root(line, PushString("first")))
        
      val result = consumeEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (Vector(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(42, 12, 77, 1, 13)
    }
    
    "evaluate descent producing a heterogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(DerefObject),
        dag.LoadLocal(line, None, Root(line, PushString("/het/het-pairs")), Het),
        Root(line, PushString("first")))
        
      val result = consumeEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (Vector(_), SDecimal(d)) => d.toInt
        case (Vector(_), SString(str)) => str
        case (Vector(_), SBoolean(b)) => b
      }
      
      result2 must contain(42, true, "daniel", 1, 13)
    }
    
    "evaluate array dereference on a homogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(DerefArray),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/arrays")), Het),
        Root(line, PushNum("2")))
        
      val result = consumeEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (Vector(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(42, 12, 77, 1, 13)
    }
    
    "evaluate array dereference on a heterogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(DerefArray),
        dag.LoadLocal(line, None, Root(line, PushString("/het/arrays")), Het),
        Root(line, PushNum("2")))
        
      val result = consumeEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (Vector(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(42, 12, 77, 1, 13)
    }
    
    "evaluate array dereference producing a heterogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(DerefArray),
        dag.LoadLocal(line, None, Root(line, PushString("/het/het-arrays")), Het),
        Root(line, PushNum("2")))
        
      val result = consumeEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (Vector(_), SDecimal(d)) => d.toInt
        case (Vector(_), SString(str)) => str
        case (Vector(_), SBoolean(b)) => b
      }
      
      result2 must contain(42, true, "daniel", 1, 13)
    }
    
    "evaluate matched binary numeric operation" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(Sub),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/pairs")), Het),
          Root(line, PushString("first"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/pairs")), Het),
          Root(line, PushString("second"))))
        
      val result = consumeEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (Vector(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(36, 12, 115, -165)
    }
    
    "evaluate matched binary numeric operation dropping undefined result" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(Div),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/pairs")), Het),
          Root(line, PushString("first"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/pairs")), Het),
          Root(line, PushString("second"))))
        
      val result = consumeEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (Vector(_), SDecimal(d)) => d.toDouble
      }
      
      result2 must contain(7, -2.026315789473684, 0.006024096385542169, 13)
    }
    
    "compute the vunion of two homogeneous sets" in {
      val line = Line(0, "")
      
      val input = Join(line, VUnion,
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers3")), Het))
        
      val result = consumeEval(input)
      
      result must haveSize(8)
      
      val result2 = result collect {
        case (Vector(_), SDecimal(d)) => d.toDouble
      }
      
      result2 must contain(42, 12, 77, 1, 13, 14, -1, 0)
    }
    
    "compute the vintersect of two homogeneous sets" in {
      val line = Line(0, "")
      
      val input = Join(line, VIntersect,
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers3")), Het))
        
      val result = consumeEval(input)
      
      result must haveSize(2)
      
      val result2 = result collect {
        case (Vector(_), SDecimal(d)) => d.toDouble
      }
      
      result2 must contain(42, 77)
    }
    
    // TODO tests for iunion and iintersect
    
    "filter homogeneous numeric set by binary operation" >> {
      "less-than" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Join(line, Map2Cross(Lt),
            dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = consumeEval(input)
        
        result must haveSize(2)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(1, 12)
      }
      
      "less-than-equal" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Join(line, Map2Cross(LtEq),
            dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = consumeEval(input)
        
        result must haveSize(3)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(12, 1, 13)
      }
      
      "greater-than" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Join(line, Map2Cross(Gt),
            dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = consumeEval(input)
        
        result must haveSize(2)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(42, 77)
      }
      
      "greater-than-equal" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Join(line, Map2Cross(GtEq),
            dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = consumeEval(input)
        
        result must haveSize(3)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(42, 77, 13)
      }
      
      "equal" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Join(line, Map2Cross(Eq),
            dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(13)
      }
      
      "not-equal" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Join(line, Map2Cross(NotEq),
            dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = consumeEval(input)
        
        result must haveSize(4)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(42, 12, 77, 1)
      }
      
      "and" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Join(line, Map2Match(And),
            Join(line, Map2Cross(NotEq),
              dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
              Root(line, PushNum("77"))),
            Join(line, Map2Cross(NotEq),
              dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
              Root(line, PushNum("13")))))
          
        val result = consumeEval(input)
        
        result must haveSize(3)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(42, 12, 1)
      }
      
      "or" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Join(line, Map2Match(Or),
            Join(line, Map2Cross(Eq),
              dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
              Root(line, PushNum("77"))),
            Join(line, Map2Cross(Eq),
              dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
              Root(line, PushNum("13")))))
          
        val result = consumeEval(input)
        
        result must haveSize(2)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(77, 13)
      }
      
      "complement of equality" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Operate(line, Comp,
            Join(line, Map2Cross(Eq),
              dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
              Root(line, PushNum("13")))))
          
        val result = consumeEval(input)
        
        result must haveSize(4)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(42, 12, 77, 1)
      }
    }
    
    "filter heterogeneous numeric set by binary operation" >> {
      "less-than" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Join(line, Map2Cross(Lt),
            dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = consumeEval(input)
        
        result must haveSize(2)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(1, 12)
      }
      
      "less-than-equal" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Join(line, Map2Cross(LtEq),
            dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = consumeEval(input)
        
        result must haveSize(3)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(12, 1, 13)
      }
      
      "greater-than" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Join(line, Map2Cross(Gt),
            dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = consumeEval(input)
        
        result must haveSize(2)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(42, 77)
      }
      
      "greater-than-equal" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Join(line, Map2Cross(GtEq),
            dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = consumeEval(input)
        
        result must haveSize(3)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(42, 77, 13)
      }
      
      "equal" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Join(line, Map2Cross(Eq),
            dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(13)
      }
      
      "not-equal" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Join(line, Map2Cross(NotEq),
            dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = consumeEval(input)
        
        result must haveSize(9)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
          case (Vector(_), SBoolean(b)) => b
          case (Vector(_), SString(str)) => str
          case (Vector(_), SObject(obj)) => obj
          case (Vector(_), SArray(arr)) => arr
        }
        
        result2 must contain(42, 12, 77, 1, true, false, "daniel",
          Map("test" -> SString("fubar")), Vector())
      }
      
      "and" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Join(line, Map2Match(And),
            Join(line, Map2Cross(NotEq),
              dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
              Root(line, PushNum("77"))),
            Join(line, Map2Cross(NotEq),
              dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
              Root(line, PushNum("13")))))
          
        val result = consumeEval(input)
        
        result must haveSize(8)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
          case (Vector(_), SBoolean(b)) => b
          case (Vector(_), SString(str)) => str
          case (Vector(_), SObject(obj)) => obj
          case (Vector(_), SArray(arr)) => arr
        }
        
        result2 must contain(42, 12, 1, true, false, "daniel",
          Map("test" -> SString("fubar")), Vector())
      }
      
      "or" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Join(line, Map2Match(Or),
            Join(line, Map2Cross(Eq),
              dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
              Root(line, PushNum("77"))),
            Join(line, Map2Cross(Eq),
              dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
              Root(line, PushNum("13")))))
          
        val result = consumeEval(input)
        
        result must haveSize(2)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(77, 13)
      }
      
      "complement of equality" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Operate(line, Comp,
            Join(line, Map2Cross(Eq),
              dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
              Root(line, PushNum("13")))))
          
        val result = consumeEval(input)
        
        result must haveSize(9)
        
        val result2 = result collect {
          case (Vector(_), SDecimal(d)) => d.toInt
          case (Vector(_), SBoolean(b)) => b
          case (Vector(_), SString(str)) => str
          case (Vector(_), SObject(obj)) => obj
          case (Vector(_), SArray(arr)) => arr
        }
        
        result2 must contain(42, 12, 77, 1, true, false, "daniel",
          Map("test" -> SString("fubar")), Vector())
      }
    }
    
    "correctly order a match following a cross" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(Mul),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
        Join(line, Map2Cross(Sub),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers3")), Het)))
          
      val result = consumeEval(input)
      
      result must haveSize(25)
      
      val result2 = result collect {
        case (Vector(_, _), SDecimal(d)) => d.toInt
      }
      
      result2 must haveSize(23)
      
      result2 must contain(0, -377, -780, 6006, -76, 5929, 1, 156, 169, 2, 1764,
        2695, 144, 1806, -360, 1176, -832, 182, 4851, -1470, -13, -41, -24)
    }
    
    "correctly order a match following a cross within a new" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(Mul),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
        Join(line, Map2Cross(Sub),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          dag.New(line, dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))))
          
      val result = consumeEval(input)
      
      result must haveSize(25)
      
      val result2 = result collect {
        case (Vector(_, _), SDecimal(d)) => d.toInt
      }
      
      result2 must haveSize(20)
      
      result2 must contain(0, 1260, -1470, 1722, 1218, -360, -780, 132, -12,
        2695, 5005, 5852, 4928, -41, -11, -76, -377, 13, -832, 156)
    }
    
    "split on a homogeneous set" in {
      val line = Line(0, "")
      
      /*
       * nums := dataset(//hom/numbers)
       * sums('n) :=
       *   m := max(nums where nums < 'n)
       *   (nums where nums = 'n) + m     -- actually, we used split root, but close enough
       * sums
       */
      val input = dag.Split(line,
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
        Join(line, Map2Cross(Add),
          SplitRoot(line, 0),
          dag.Reduce(line, Max,
            Filter(line, None, None,
              dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
              Join(line, Map2Cross(Lt),
                dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
                SplitRoot(line, 0))))))
              
      val result = consumeEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (Vector(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(55, 13, 119, 25)
    }
    
    "evaluate a histogram function" in {
      val Expected = Map("daniel" -> 9, "kris" -> 8, "derek" -> 7, "nick" -> 18,
        "john" -> 14, "alissa" -> 7, "franco" -> 14, "matthew" -> 10, "jason" -> 13)
      
      val line = Line(0, "")
      
      /*
       * clicks := dataset(//clicks)
       * histogram('user) :=
       *   { user: 'user, num: count(clicks where clicks.user = 'user) }
       * histogram
       */
      val input = dag.Split(line,
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, None, Root(line, PushString("/clicks")), Het),
          Root(line, PushString("user"))),
        Join(line, Map2Cross(JoinObject),
          Join(line, Map2Cross(WrapObject),
            Root(line, PushString("user")),
            SplitRoot(line, 0)),
          Join(line, Map2Cross(WrapObject),
            Root(line, PushString("num")),
            dag.Reduce(line, Count,
              Filter(line, None, None,
                dag.LoadLocal(line, None, Root(line, PushString("/clicks")), Het),
                Join(line, Map2Cross(Eq),
                  Join(line, Map2Cross(DerefObject),
                    dag.LoadLocal(line, None, Root(line, PushString("/clicks")), Het),
                    Root(line, PushString("user"))),
                  SplitRoot(line, 0)))))))
                  
      val result = consumeEval(input)
      
      result must haveSize(9)
      
      result foreach {
        case (Vector(_), SObject(obj)) => {
          obj must haveKey("user")
          obj must haveKey("num")
          
          obj("user") must beLike {
            case SString(str) => {
              str must beOneOf("daniel", "kris", "derek", "nick", "john",
                "alissa", "franco", "matthew", "jason")
            }
          }
          val SString(user) = obj("user")
            
          obj("num") must beLike {
            case SDecimal(d) => d mustEqual Expected(user)
          }
        }
        
        case p => failure("'%s' does not match the expected pattern".format(p))
      }
    }
    
    "reduce homogeneous sets" >> {
      "count" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Count,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(5)
      }
      
      "mean" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Mean,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(29)
      }
      
      "median" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Median,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(13)
      }.pendingUntilFixed
      
      "mode" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Mode,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers2")), Het))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(1)
      }.pendingUntilFixed
      
      "max" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Max,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(77)
      }
      
      "min" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Min,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(1)
      }
      
      "standard deviation" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, StdDev,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SDecimal(d)) => d.toDouble
        }
        
        result2 must contain(27.575351312358652)
      }
      
      "sum" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Sum,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(145)
      }
    }
    
    "reduce heterogeneous sets" >> {
      "count" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Count,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(10)
      }
      
      "mean" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Mean,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(29)
      }
      
      "median" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Median,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(13)
      }.pendingUntilFixed
      
      "mode" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Mode,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers2")), Het))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(1)
      }.pendingUntilFixed
      
      "max" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Max,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(77)
      }
      
      "min" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Min,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(1)
      }
      
      "standard deviation" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, StdDev,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SDecimal(d)) => d.toDouble
        }
        
        result2 must contain(27.575351312358652)
      }
      
      "sum" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Sum,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
          
        val result = consumeEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (Vector(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(145)
      }
    }
  }
}
