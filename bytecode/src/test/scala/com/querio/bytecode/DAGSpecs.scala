/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.querio.bytecode

import org.specs2.mutable._

object DAGSpecs extends Specification with DAG {
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
    
    "parse a single-level split" in {
      val line = Line(0, "")
      val result = decorate(Vector(line, PushTrue, instructions.Split, PushFalse, VUnion, Merge))
      result mustEqual Right(dag.Split(line, Root(line, PushTrue), Join(line, VUnion, SplitRoot(line, 0), Root(line, PushFalse))))
    }
    
    "parse a bi-level split" in {
      val line = Line(0, "")
      val result = decorate(Vector(line, PushTrue, instructions.Split, instructions.Split, PushFalse, VUnion, Merge, Merge))
      result mustEqual Right(dag.Split(line, Root(line, PushTrue), dag.Split(line, SplitRoot(line, 0), Join(line, VUnion, SplitRoot(line, 0), Root(line, PushFalse)))))
    }
    
    "accept split which reduces the stack" in {
      val line = Line(0, "")
      val result = decorate(Vector(
        line,
        PushTrue,
        PushFalse,
        instructions.Split,
        Map2Match(Add),
        Merge))
        
      result mustEqual Right(
        dag.Split(line,
          Root(line, PushFalse),
          Join(line, Map2Match(Add),
            Root(line, PushTrue),
            SplitRoot(line, 0))))
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
      result mustEqual Right(Filter(line, false, None, Root(line, PushFalse), Root(line, PushTrue)))
    }
    
    "parse a filter with a non-null predicate" in {
      val line = Line(0, "")
      
      "simple range" >> {
        val result = decorate(Vector(line, PushNum("12"), PushNum("42"), PushFalse, PushTrue, FilterMatch(2, Some(Vector(Range)))))
        result mustEqual Right(Filter(line, false, Some(Contiguous(ValueOperand(Root(line, PushNum("12"))), ValueOperand(Root(line, PushNum("42"))))), Root(line, PushFalse), Root(line, PushTrue)))
      }
      
      "complemented range" >> {
        val result = decorate(Vector(line, PushNum("12"), PushNum("42"), PushFalse, PushTrue, FilterMatch(2, Some(Vector(Range, Comp)))))
        result mustEqual Right(Filter(line, false, Some(Complementation(Contiguous(ValueOperand(Root(line, PushNum("12"))), ValueOperand(Root(line, PushNum("42")))))), Root(line, PushFalse), Root(line, PushTrue)))
      }
      
      "range with addend" >> {
        val result = decorate(Vector(line, PushNum("42"), PushNum("10"), PushNum("12"), PushFalse, PushTrue, FilterMatch(3, Some(Vector(Add, Range)))))
        result mustEqual Right(
          Filter(line, false, Some(
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
          Filter(line, false, Some(
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
          Filter(line, false, Some(
            Contiguous(
              ValueOperand(Root(line, PushNum("42"))),
              PropertyOperand(Root(line, PushString("foo"))))),
            Root(line, PushFalse), Root(line, PushTrue)))
      }
      
      "range with array selector" >> {
        val result = decorate(Vector(line, PushNum("42"), PushNum("12"), PushFalse, PushTrue, FilterMatch(2, Some(Vector(DerefArray, Range)))))
        result mustEqual Right(
          Filter(line, false, Some(
            Contiguous(
              ValueOperand(Root(line, PushNum("42"))),
              IndexOperand(Root(line, PushNum("12"))))),
            Root(line, PushFalse), Root(line, PushTrue)))
      }
      
      // TODO more complicated stuff requires swap and dup
    }
    
    "continue processing beyond a filter" in {
      val line = Line(0, "")
      val result = decorate(Vector(line, PushFalse, PushTrue, FilterMatch(0, None), Map1(Neg)))
      result mustEqual Right(
        Operate(line, Neg,
          Filter(line, false, None,
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
        val instr = instructions.Split
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
      decorate(Vector(Line(0, ""), PushTrue, instructions.Split, PushFalse, Merge)) mustEqual Left(MergeWithUnmatchedTails)
    }
    
    "reject unmatched merge" in {
      decorate(Vector(Line(0, ""), PushTrue, Merge)) mustEqual Left(UnmatchedMerge)
    }
    
    "reject split without corresponding merge" in {
      decorate(Vector(Line(0, ""), PushTrue, instructions.Split)) mustEqual Left(UnmatchedSplit)
    }
    
    "reject split which increases the stack" in {
      val line = Line(0, "")
      val result = decorate(Vector(
        line,
        PushTrue,
        PushFalse,
        instructions.Split,
        PushTrue,
        Merge))
        
      result mustEqual Left(MergeWithUnmatchedTails)
    }
  }
}
