package com.precog
package daze

import org.specs2.mutable._
import bytecode._

object DAGSpecs extends Specification with DAG with Stdlib {
  import instructions._
  import dag._
  
  "dag decoration" should {
    "recognize root instructions" in {
      "push_str" >> {
        decorate(Vector(Line(0, ""), PushString("test"))) mustEqual Right(Root(Line(0, ""), PushString("test")))
      }
      
      "push_num" >> {
        decorate(Vector(Line(0, ""), PushNum("42"))) mustEqual Right(Root(Line(0, ""), PushNum("42")))
      }
      
      "push_true" >> {
        decorate(Vector(Line(0, ""), PushTrue)) mustEqual Right(Root(Line(0, ""), PushTrue))
      }
      
      "push_false" >> {
        decorate(Vector(Line(0, ""), PushFalse)) mustEqual Right(Root(Line(0, ""), PushFalse))
      }
      
      "push_object" >> {
        decorate(Vector(Line(0, ""), PushObject)) mustEqual Right(Root(Line(0, ""), PushObject))
      }
      
      "push_array" >> {
        decorate(Vector(Line(0, ""), PushArray)) mustEqual Right(Root(Line(0, ""), PushArray))
      }
    }
    
    "recognize a new instruction" in {
      decorate(Vector(Line(0, ""), PushNum("5"), Map1(instructions.New))) mustEqual Right(dag.New(Line(0, ""), Root(Line(0, ""), PushNum("5"))))
    }
    
    "parse out load_local" in {
      val result = decorate(Vector(Line(0, ""), PushString("/foo"), instructions.LoadLocal(Het)))
      result mustEqual Right(dag.LoadLocal(Line(0, ""), None, Root(Line(0, ""), PushString("/foo")), Het))
    }
    
    "parse out map1" in {
      val result = decorate(Vector(Line(0, ""), PushTrue, Map1(Neg)))
      result mustEqual Right(Operate(Line(0, ""), Neg, Root(Line(0, ""), PushTrue)))
    }
    
    "parse out reduce" in {
      val result = decorate(Vector(Line(0, ""), PushTrue, instructions.Reduce(Count)))
      result mustEqual Right(dag.Reduce(Line(0, ""), Count, Root(Line(0, ""), PushTrue)))
    }    

    "parse out set-reduce" in {
      val result = decorate(Vector(Line(0, ""), PushTrue, instructions.SetReduce(Distinct)))
      result mustEqual Right(dag.SetReduce(Line(0, ""), Distinct, Root(Line(0, ""), PushTrue)))
    }
    
    "parse a single-level split" in {
      val line = Line(0, "")
      
      val result = decorate(Vector(
        line,
        PushTrue,
        Dup,
        Bucket,
        instructions.Split(1, 2),
        VUnion,
        Merge))
        
      result must beLike {
        case Right(
          s @ dag.Split(`line`,
            Vector(SingleBucketSpec(
              Root(`line`, PushTrue),
              Root(`line`, PushTrue))),
            Join(`line`, VUnion, 
              sg @ SplitGroup(`line`, 1, Vector()),
              sp @ SplitParam(`line`, 0)))) => {
              
          sp.parent mustEqual s
          sg.parent mustEqual s
        }
      }
    }
    
    "parse a bi-level split" in {
      val line = Line(0, "")
      
      val result = decorate(Vector(
        line,
        PushTrue,
        PushFalse,
        Bucket,
        instructions.Split(1, 2),
        Bucket,
        instructions.Split(1, 2),
        VUnion,
        Merge,
        Merge))
        
      result must beLike {
        case Right(
          s1 @ dag.Split(`line`,
            Vector(SingleBucketSpec(Root(`line`, PushTrue), Root(`line`, PushFalse))),
            s2 @ dag.Split(`line`,
              Vector(SingleBucketSpec(sg1 @ SplitGroup(`line`, 1, Vector()), sp1 @ SplitParam(`line`, 0))),
              Join(`line`, VUnion,
                sg2 @ SplitGroup(`line`, 1, Vector()),
                sp2 @ SplitParam(`line`, 0))))) => {
          
          sp1.parent mustEqual s1
          sg1.parent mustEqual s1
          
          sp2.parent mustEqual s2
          sg2.parent mustEqual s2
        }
      }
    }
    
    "parse a bi-level split with intermediate usage" in {
      val line = Line(0, "")
      
      val result = decorate(Vector(
        line,
        PushTrue,
        PushFalse,
        Bucket,
        instructions.Split(1, 2),
        Map2Cross(Add),
        PushNum("42"),
        PushFalse,
        Bucket,
        instructions.Split(1, 2),
        Drop,
        VUnion,
        Merge,
        Merge))
      
      result must beLike {
        case Right(
          s1 @ dag.Split(`line`,
            Vector(SingleBucketSpec(Root(`line`, PushTrue), Root(`line`, PushFalse))),
            s2 @ dag.Split(`line`,
              Vector(SingleBucketSpec(Root(`line`, PushNum("42")), Root(`line`, PushFalse))),
              Join(`line`, VUnion,
                Join(`line`, Map2Cross(Add),
                  sg1 @ SplitGroup(`line`, 1, Vector()),
                  sp1 @ SplitParam(`line`, 0)),
                sg2 @ SplitGroup(`line`, 1, Vector()))))) => {
          
          sp1.parent mustEqual s1
          sg1.parent mustEqual s1
          
          sg2.parent mustEqual s2
        }
      } 
    }
    
    "parse a split with merged buckets" >> {
      "union" >> {
        val line = Line(0, "")
        
        val result = decorate(Vector(
          line,
          PushNum("1"),
          PushNum("2"),
          Bucket,
          PushNum("3"),
          PushNum("4"),
          Bucket,
          MergeBuckets(false),
          instructions.Split(1, 2),
          IUnion,
          Merge))
          
        result must beLike {
          case Right(
            s @ dag.Split(`line`,
              Vector(MergeBucketSpec(
                SingleBucketSpec(Root(`line`, PushNum("1")), Root(`line`, PushNum("2"))),
                SingleBucketSpec(Root(`line`, PushNum("3")), Root(`line`, PushNum("4"))), false)),
              Join(`line`, IUnion,
                sg @ SplitGroup(`line`, 1, Vector()),
                sp @ SplitParam(`line`, 0)))) => {
            
            sg.parent mustEqual s
            sp.parent mustEqual s
          }
        }
      }
      
      "intersect" >> {
        val line = Line(0, "")
        
        val result = decorate(Vector(
          line,
          PushNum("1"),
          PushNum("2"),
          Bucket,
          PushNum("3"),
          PushNum("4"),
          Bucket,
          MergeBuckets(true),
          instructions.Split(1, 2),
          IUnion,
          Merge))
          
        result must beLike {
          case Right(
            s @ dag.Split(`line`,
              Vector(MergeBucketSpec(
                SingleBucketSpec(Root(`line`, PushNum("1")), Root(`line`, PushNum("2"))),
                SingleBucketSpec(Root(`line`, PushNum("3")), Root(`line`, PushNum("4"))), true)),
              Join(`line`, IUnion,
                sg @ SplitGroup(`line`, 1, Vector()),
                sp @ SplitParam(`line`, 0)))) => {
            
            sg.parent mustEqual s
            sp.parent mustEqual s
          }
        }
      }
    }
    
    "parse a split with zipped buckets" in {
      val line = Line(0, "")
      
      val result = decorate(Vector(
        line,
        PushNum("1"),
        PushNum("2"),
        Bucket,
        PushNum("3"),
        PushNum("4"),
        Bucket,
        ZipBuckets,
        instructions.Split(1, 3),
        IUnion,
        IUnion,
        Merge))
        
      result must beLike {
        case Right(
          s @ dag.Split(`line`,
            Vector(ZipBucketSpec(
              SingleBucketSpec(Root(`line`, PushNum("1")), Root(`line`, PushNum("2"))),
              SingleBucketSpec(Root(`line`, PushNum("3")), Root(`line`, PushNum("4"))))),
            Join(`line`, IUnion,
              sg2 @ SplitGroup(`line`, 2, Vector()),
              Join(`line`, IUnion,
                sg1 @ SplitGroup(`line`, 1, Vector()),
                sp1 @ SplitParam(`line`, 0))))) => {
          
          sg1.parent mustEqual s
          sp1.parent mustEqual s
          sg2.parent mustEqual s
        }
      }
    }
    
    "accept split which reduces the stack" in {
      val line = Line(0, "")
      
      val result = decorate(Vector(
        line,
        PushNum("42"),
        PushTrue,
        PushFalse,
        Bucket,
        instructions.Split(1, 2),
        Drop,
        Map2Match(Add),
        Merge))
        
      result must beLike {
        case Right(
          s @ dag.Split(`line`,
            Vector(SingleBucketSpec(Root(`line`, PushTrue), Root(`line`, PushFalse))),
            Join(`line`, Map2Match(Add),
              Root(`line`, PushNum("42")),
              sg @ SplitGroup(`line`, 1, Vector())))) => {
          
          sg.parent mustEqual s
        }
      }
    }
    
    "recognize a join instruction" in {
      "map2_match" >> {
        val line = Line(0, "")
        val result = decorate(Vector(line, PushTrue, PushFalse, Map2Match(Add)))
        result mustEqual Right(Join(line, Map2Match(Add), Root(line, PushTrue), Root(line, PushFalse)))
      }
      
      "map2_cross" >> {
        val line = Line(0, "")
        val result = decorate(Vector(line, PushTrue, PushFalse, Map2Cross(Add)))
        result mustEqual Right(Join(line, Map2Cross(Add), Root(line, PushTrue), Root(line, PushFalse)))
      }
      
      "vunion" >> {
        val line = Line(0, "")
        val result = decorate(Vector(line, PushTrue, PushFalse, VUnion))
        result mustEqual Right(Join(line, VUnion, Root(line, PushTrue), Root(line, PushFalse)))
      }
      
      "vintersect" >> {
        val line = Line(0, "")
        val result = decorate(Vector(line, PushTrue, PushFalse, VIntersect))
        result mustEqual Right(Join(line, VIntersect, Root(line, PushTrue), Root(line, PushFalse)))
      }
      
      "iunion" >> {
        val line = Line(0, "")
        val result = decorate(Vector(line, PushTrue, PushFalse, IUnion))
        result mustEqual Right(Join(line, IUnion, Root(line, PushTrue), Root(line, PushFalse)))
      }
      
      "iintersect" >> {
        val line = Line(0, "")
        val result = decorate(Vector(line, PushTrue, PushFalse, IIntersect))
        result mustEqual Right(Join(line, IIntersect, Root(line, PushTrue), Root(line, PushFalse)))
      }
    }
    
    "parse a filter with null predicate" in {
      val line = Line(0, "")
      val result = decorate(Vector(line, PushFalse, PushTrue, FilterMatch(0, None)))
      result mustEqual Right(Filter(line, None, None, Root(line, PushFalse), Root(line, PushTrue)))
    }
    
    "parse a filter with a non-null predicate" in {
      val line = Line(0, "")
      
      "simple range" >> {
        val result = decorate(Vector(line, PushNum("12"), PushNum("42"), PushFalse, PushTrue, FilterMatch(2, Some(Vector(Range)))))
        result mustEqual Right(Filter(line, None, Some(Contiguous(ValueOperand(Root(line, PushNum("12"))), ValueOperand(Root(line, PushNum("42"))))), Root(line, PushFalse), Root(line, PushTrue)))
      }
      
      "complemented range" >> {
        val result = decorate(Vector(line, PushNum("12"), PushNum("42"), PushFalse, PushTrue, FilterMatch(2, Some(Vector(Range, Comp)))))
        result mustEqual Right(Filter(line, None, Some(Complementation(Contiguous(ValueOperand(Root(line, PushNum("12"))), ValueOperand(Root(line, PushNum("42")))))), Root(line, PushFalse), Root(line, PushTrue)))
      }
      
      "range with addend" >> {
        val result = decorate(Vector(line, PushNum("42"), PushNum("10"), PushNum("12"), PushFalse, PushTrue, FilterMatch(3, Some(Vector(Add, Range)))))
        result mustEqual Right(
          Filter(line, None, Some(
            Contiguous(
              ValueOperand(Root(line, PushNum("42"))),
              BinaryOperand(
                ValueOperand(Root(line, PushNum("10"))),
                Add,
                ValueOperand(Root(line, PushNum("12")))))),
            Root(line, PushFalse), Root(line, PushTrue)))
      }
      
      "range with negation" >> {
        val result = decorate(Vector(line, PushNum("42"), PushNum("12"), PushFalse, PushTrue, FilterMatch(2, Some(Vector(Neg, Range)))))
        result mustEqual Right(
          Filter(line, None, Some(
            Contiguous(
              ValueOperand(Root(line, PushNum("42"))),
              UnaryOperand(
                Neg,
                ValueOperand(Root(line, PushNum("12")))))),
            Root(line, PushFalse), Root(line, PushTrue)))
      }
      
      "range with object selector" >> {
        val result = decorate(Vector(line, PushNum("42"), PushString("foo"), PushFalse, PushTrue, FilterMatch(2, Some(Vector(DerefObject, Range)))))
        result mustEqual Right(
          Filter(line, None, Some(
            Contiguous(
              ValueOperand(Root(line, PushNum("42"))),
              PropertyOperand(Root(line, PushString("foo"))))),
            Root(line, PushFalse), Root(line, PushTrue)))
      }
      
      "range with array selector" >> {
        val result = decorate(Vector(line, PushNum("42"), PushNum("12"), PushFalse, PushTrue, FilterMatch(2, Some(Vector(DerefArray, Range)))))
        result mustEqual Right(
          Filter(line, None, Some(
            Contiguous(
              ValueOperand(Root(line, PushNum("42"))),
              IndexOperand(Root(line, PushNum("12"))))),
            Root(line, PushFalse), Root(line, PushTrue)))
      }
      
      // TODO more complicated stuff requires swap and dup
    }
    
    "parse a filter_cross" in {
      val line = Line(0, "")
      val result = decorate(Vector(line, PushTrue, PushFalse, FilterCross(0, None)))
      result mustEqual Right(Filter(line, Some(CrossNeutral), None, Root(line, PushTrue), Root(line, PushFalse)))
    }
    
    "parse a filter_crossl" in {
      val line = Line(0, "")
      val result = decorate(Vector(line, PushTrue, PushFalse, FilterCrossLeft(0, None)))
      result mustEqual Right(Filter(line, Some(CrossLeft), None, Root(line, PushTrue), Root(line, PushFalse)))
    }
    
    "parse a filter_crossr" in {
      val line = Line(0, "")
      val result = decorate(Vector(line, PushTrue, PushFalse, FilterCrossRight(0, None)))
      result mustEqual Right(Filter(line, Some(CrossRight), None, Root(line, PushTrue), Root(line, PushFalse)))
    }
    
    "continue processing beyond a filter" in {
      val line = Line(0, "")
      val result = decorate(Vector(line, PushFalse, PushTrue, FilterMatch(0, None), Map1(Neg)))
      result mustEqual Right(
        Operate(line, Neg,
          Filter(line, None, None,
            Root(line, PushFalse),
            Root(line, PushTrue))))
    }
    
    "parse and factor a dup" in {
      {
        val line = Line(0, "")
        val result = decorate(Vector(line, PushTrue, Dup, VUnion))
        result mustEqual Right(Join(line, VUnion, Root(line, PushTrue), Root(line, PushTrue)))
      }
      
      {
        val line = Line(0, "")
        val result = decorate(Vector(line, PushNum("42"), Map1(Neg), Dup, VUnion))
        result mustEqual Right(Join(line, VUnion, Operate(line, Neg, Root(line, PushNum("42"))), Operate(line, Neg, Root(line, PushNum("42")))))
      }
    }
    
    "parse and factor a swap" in {
      "1" >> {
        val line = Line(0, "")
        val result = decorate(Vector(line, PushFalse, PushTrue, Swap(1), VUnion))
        result mustEqual Right(Join(line, VUnion, Root(line, PushTrue), Root(line, PushFalse)))
      }
      
      "3" >> {
        val line = Line(0, "")
        val result = decorate(Vector(line, PushTrue, PushString("foo"), PushFalse, PushNum("42"), Swap(3), VUnion, VUnion, VUnion))
        result mustEqual Right(
          Join(line, VUnion,
            Root(line, PushNum("42")),
            Join(line, VUnion,
              Root(line, PushString("foo")),
              Join(line, VUnion, Root(line, PushFalse), Root(line, PushTrue)))))
      }
    }
    
    // TODO line info
  }
  
  "stream validation" should {
    "reject the empty stream" in {
      decorate(Vector()) mustEqual Left(EmptyStream)
    }
    
    "reject a line-less stream" in {
      decorate(Vector(PushTrue)) mustEqual Left(UnknownLine)
      decorate(Vector(PushTrue, PushFalse, Map1(Comp))) mustEqual Left(UnknownLine)
    }
    
    "detect stack underflow" in {
      "map1" >> {     // historic sidebar: since we don't have pop, this is the *only* map1 underflow case!
        val instr = Map1(Neg)
        decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }
      
      "map2_match" >> {
        {
          val instr = Map2Match(Add)
          decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
        }
        
        {
          val instr = Map2Match(Add)
          decorate(Vector(Line(0, ""), PushTrue, instr)) mustEqual Left(StackUnderflow(instr))
        }
        
        {
          val instr = Map2Match(Add)
          decorate(Vector(Line(0, ""), PushTrue, Map1(Comp), instr, Map2Match(Sub))) mustEqual Left(StackUnderflow(instr))
        }
      }
      
      "map2_cross" >> {
        {
          val instr = Map2Cross(Add)
          decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
        }
        
        {
          val instr = Map2Cross(Add)
          decorate(Vector(Line(0, ""), PushTrue, instr)) mustEqual Left(StackUnderflow(instr))
        }
        
        {
          val instr = Map2Cross(Add)
          decorate(Vector(Line(0, ""), PushTrue, Map1(Comp), instr, Map2Cross(Sub))) mustEqual Left(StackUnderflow(instr))
        }
      }
      
      "reduce" >> {     // similar to map1, only one underflow case!
        val instr = instructions.Reduce(Count)
        decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }      

      "set-reduce" >> {     // similar to map1, only one underflow case!
        val instr = instructions.SetReduce(Distinct)
        decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }
      
      "vunion" >> {     // similar to map1, only one underflow case!
        val instr = VUnion
        decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }
      
      "vintersect" >> {     // similar to map1, only one underflow case!
        val instr = VIntersect
        decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }
      
      "iunion" >> {     // similar to map1, only one underflow case!
        val instr = IUnion
        decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }
      
      "iintersect" >> {     // similar to map1, only one underflow case!
        val instr = IIntersect
        decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }
      
      "split" >> {     // similar to map1, only one underflow case!
        val instr = instructions.Split(1, 2)
        decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }
      
      // merge cannot stack underflow; curious, no?
      
      "filter_match" >> {
        {
          val instr = FilterMatch(0, None)
          decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
        }
        
        {
          val instr = FilterMatch(0, None)
          decorate(Vector(Line(0, ""), PushTrue, instr)) mustEqual Left(StackUnderflow(instr))
        }
        
        {
          val instr = FilterMatch(0, None)
          decorate(Vector(Line(0, ""), PushTrue, PushTrue, Map2Match(Add), instr)) mustEqual Left(StackUnderflow(instr))
        }
      }
      
      "filter_cross" >> {
        {
          val instr = FilterCross(0, None)
          decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
        }
        
        {
          val instr = FilterCross(0, None)
          decorate(Vector(Line(0, ""), PushTrue, instr)) mustEqual Left(StackUnderflow(instr))
        }
        
        {
          val instr = FilterCross(0, None)
          decorate(Vector(Line(0, ""), PushTrue, PushTrue, Map2Match(Add), instr)) mustEqual Left(StackUnderflow(instr))
        }
      }
      
      "dup" >> {
        decorate(Vector(Line(0, ""), Dup)) mustEqual Left(StackUnderflow(Dup))
      }
      
      "swap" >> {
        {
          val instr = Swap(1)
          decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
        }
        
        {
          val instr = Swap(1)
          decorate(Vector(Line(0, ""), PushTrue, instr)) mustEqual Left(StackUnderflow(instr))
        }
        
        {
          val instr = Swap(2)
          decorate(Vector(Line(0, ""), PushTrue, PushTrue, instr)) mustEqual Left(StackUnderflow(instr))
        }
        
        {
          val instr = Swap(5)
          decorate(Vector(Line(0, ""), PushTrue, PushTrue, instr)) mustEqual Left(StackUnderflow(instr))
        }
      }
      
      "load_local" >> {
        val instr = instructions.LoadLocal(Het)
        decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }
    }
    
    "reject multiple stack values at end" in {
      decorate(Vector(Line(0, ""), PushTrue, PushFalse)) mustEqual Left(MultipleStackValuesAtEnd)
      decorate(Vector(Line(0, ""), PushTrue, PushFalse, PushNum("42"))) mustEqual Left(MultipleStackValuesAtEnd)
      decorate(Vector(Line(0, ""), PushTrue, PushFalse, PushNum("42"), PushString("foo"))) mustEqual Left(MultipleStackValuesAtEnd)
    }
    
    "reject a negative predicate depth" in {
      "filter_match" >> {
        {
          val instr = FilterMatch(-1, None)
          decorate(Vector(Line(0, ""), PushTrue, PushFalse, instr)) mustEqual Left(NegativePredicateDepth(instr))
        }
        
        {
          val instr = FilterMatch(-255, None)
          decorate(Vector(Line(0, ""), PushTrue, PushFalse, instr)) mustEqual Left(NegativePredicateDepth(instr))
        }
      }
      
      "filter_cross" >> {
        {
          val instr = FilterCross(-1, None)
          decorate(Vector(Line(0, ""), PushTrue, PushFalse, instr)) mustEqual Left(NegativePredicateDepth(instr))
        }
        
        {
          val instr = FilterCross(-255, None)
          decorate(Vector(Line(0, ""), PushTrue, PushFalse, instr)) mustEqual Left(NegativePredicateDepth(instr))
        }
      }
    }
    
    "detect predicate stack underflow" in {
      "add" >> {
        val instr = FilterMatch(1, Some(Vector(Add)))
        decorate(Vector(Line(0, ""), PushTrue, PushFalse, PushNum("42"), instr)) mustEqual Left(PredicateStackUnderflow(instr))
      }
      
      "sub" >> {
        val instr = FilterMatch(1, Some(Vector(Sub)))
        decorate(Vector(Line(0, ""), PushTrue, PushFalse, PushNum("42"), instr)) mustEqual Left(PredicateStackUnderflow(instr))
      }
      
      "mul" >> {
        val instr = FilterMatch(1, Some(Vector(Mul)))
        decorate(Vector(Line(0, ""), PushTrue, PushFalse, PushNum("42"), instr)) mustEqual Left(PredicateStackUnderflow(instr))
      }
      
      "div" >> {
        val instr = FilterMatch(1, Some(Vector(Div)))
        decorate(Vector(Line(0, ""), PushTrue, PushFalse, PushNum("42"), instr)) mustEqual Left(PredicateStackUnderflow(instr))
      }
      
      "or" >> {
        val instr = FilterMatch(1, Some(Vector(Or)))
        decorate(Vector(Line(0, ""), PushTrue, PushFalse, PushNum("42"), instr)) mustEqual Left(PredicateStackUnderflow(instr))
      }
      
      "and" >> {
        val instr = FilterMatch(1, Some(Vector(And)))
        decorate(Vector(Line(0, ""), PushTrue, PushFalse, PushNum("42"), instr)) mustEqual Left(PredicateStackUnderflow(instr))
      }
      
      "comp" >> {
        val instr = FilterMatch(0, Some(Vector(Comp)))
        decorate(Vector(Line(0, ""), PushFalse, PushNum("42"), instr)) mustEqual Left(PredicateStackUnderflow(instr))
      }
      
      "neg" >> {
        val instr = FilterMatch(0, Some(Vector(Comp)))
        decorate(Vector(Line(0, ""), PushFalse, PushNum("42"), instr)) mustEqual Left(PredicateStackUnderflow(instr))
      }
      
      "deref_object" >> {
        val instr = FilterMatch(0, Some(Vector(DerefObject)))
        decorate(Vector(Line(0, ""), PushFalse, PushNum("42"), instr)) mustEqual Left(PredicateStackUnderflow(instr))
      }
      
      "deref_array" >> {
        val instr = FilterMatch(0, Some(Vector(DerefArray)))
        decorate(Vector(Line(0, ""), PushFalse, PushNum("42"), instr)) mustEqual Left(PredicateStackUnderflow(instr))
      }
      
      "range" >> {
        val instr = FilterMatch(1, Some(Vector(Range)))
        decorate(Vector(Line(0, ""), PushTrue, PushFalse, PushNum("42"), instr)) mustEqual Left(PredicateStackUnderflow(instr))
      }
    }
    
    "reject multiple predicate stack values at end" in {
      val instr = FilterMatch(3, Some(Vector(Range)))
      decorate(Vector(Line(0, ""), PushTrue, PushTrue, PushTrue, PushFalse, PushNum("42"), instr)) mustEqual Left(MultiplePredicateStackValuesAtEnd(instr))
    }
    
    "reject non-range value at end" in {
      val instr = FilterMatch(2, Some(Vector(Add)))
      decorate(Vector(Line(0, ""), PushTrue, PushTrue, PushFalse, PushNum("42"), instr)) mustEqual Left(NonRangePredicateStackAtEnd(instr))
    }
    
    "reject an operand operation applied to a range" in {
      "add" >> {
        val instr = FilterMatch(3, Some(Vector(Range, Add)))
        decorate(Vector(Line(0, ""), PushTrue, PushTrue, PushTrue, PushFalse, PushNum("42"), instr)) mustEqual Left(OperandOpAppliedToRange(instr))
      }
      
      "sub" >> {
        val instr = FilterMatch(3, Some(Vector(Range, Sub)))
        decorate(Vector(Line(0, ""), PushTrue, PushTrue, PushTrue, PushFalse, PushNum("42"), instr)) mustEqual Left(OperandOpAppliedToRange(instr))
      }
      
      "mul" >> {
        val instr = FilterMatch(3, Some(Vector(Range, Mul)))
        decorate(Vector(Line(0, ""), PushTrue, PushTrue, PushTrue, PushFalse, PushNum("42"), instr)) mustEqual Left(OperandOpAppliedToRange(instr))
      }
      
      "div" >> {
        val instr = FilterMatch(3, Some(Vector(Range, Div)))
        decorate(Vector(Line(0, ""), PushTrue, PushTrue, PushTrue, PushFalse, PushNum("42"), instr)) mustEqual Left(OperandOpAppliedToRange(instr))
      }
      
      "range" >> {
        val instr = FilterMatch(3, Some(Vector(Range, Range)))
        decorate(Vector(Line(0, ""), PushTrue, PushTrue, PushTrue, PushFalse, PushNum("42"), instr)) mustEqual Left(OperandOpAppliedToRange(instr))
      }
      
      "neg" >> {
        val instr = FilterMatch(2, Some(Vector(Range, Neg)))
        decorate(Vector(Line(0, ""), PushTrue, PushTrue, PushFalse, PushNum("42"), instr)) mustEqual Left(OperandOpAppliedToRange(instr))
      }
    }
    
    "reject a range operation applied to an operand" in {
      "or" >> {
        val instr = FilterMatch(2, Some(Vector(Or)))
        decorate(Vector(Line(0, ""), PushTrue, PushTrue, PushFalse, PushNum("42"), instr)) mustEqual Left(RangeOpAppliedToOperand(instr))
      }
      
      "and" >> {
        val instr = FilterMatch(2, Some(Vector(And)))
        decorate(Vector(Line(0, ""), PushTrue, PushTrue, PushFalse, PushNum("42"), instr)) mustEqual Left(RangeOpAppliedToOperand(instr))
      }
      
      "comp" >> {
        val instr = FilterMatch(1, Some(Vector(Comp)))
        decorate(Vector(Line(0, ""), PushTrue, PushFalse, PushNum("42"), instr)) mustEqual Left(RangeOpAppliedToOperand(instr))
      }
    }
    
    // TODO predicate typing
    
    "reject negative swap depth" in {
      {
        val instr = Swap(-1)
        decorate(Vector(Line(0, ""), PushTrue, instr)) mustEqual Left(NonPositiveSwapDepth(instr))
      }
      
      {
        val instr = Swap(-255)
        decorate(Vector(Line(0, ""), PushTrue, instr)) mustEqual Left(NonPositiveSwapDepth(instr))
      }
    }
    
    "reject zero swap depth" in {
      val instr = Swap(0)
      decorate(Vector(Line(0, ""), PushTrue, instr)) mustEqual Left(NonPositiveSwapDepth(instr))
    }
    
    "reject merge with deepened stack" in {
      decorate(Vector(
        Line(0, ""),
        PushTrue,
        PushFalse,
        Bucket,
        instructions.Split(1, 2),
        PushFalse,
        Merge,
        Drop,
        Drop)) mustEqual Left(MergeWithUnmatchedTails)
    }
    
    "accept merge with reduced (but reordered) stack" in {
      val line = Line(0, "")
      
      val result = decorate(Vector(
        line,
        PushTrue,
        PushFalse,
        PushNum("42"),
        PushNum("12"),
        Bucket,
        instructions.Split(1, 2),
        Drop,
        Swap(1),
        Swap(2),
        VUnion,
        Merge,
        VIntersect))
      
      lazy val split: dag.Split = dag.Split(line,
        Vector(SingleBucketSpec(Root(line, PushNum("42")), Root(line, PushNum("12")))),
        Join(line, VUnion,
          SplitGroup(line, 1, Vector())(split),
          Root(line, PushTrue)))
      
      val expect = Join(line, VIntersect, Root(line, PushFalse), split)
        
      result mustEqual Right(expect)
    }
    
    "reject unmatched merge" in {
      decorate(Vector(Line(0, ""), PushTrue, Merge)) mustEqual Left(UnmatchedMerge)
    }
    
    "reject split without corresponding merge" in {
      decorate(Vector(Line(0, ""),
        PushTrue,
        PushFalse,
        Bucket,
        instructions.Split(1, 2))) mustEqual Left(UnmatchedSplit)
    }
    
    "reject split which increases the stack" in {
      val line = Line(0, "")
      
      val result = decorate(Vector(
        line,
        PushTrue,
        PushFalse,
        Bucket,
        instructions.Split(1, 2),
        PushTrue,
        Merge))
        
      result mustEqual Left(MergeWithUnmatchedTails)
    }
  }
}
