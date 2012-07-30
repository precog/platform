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
package com.precog
package daze

import org.specs2.mutable._
import bytecode._

object DAGSpecs extends Specification with DAG with RandomLibrary {
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

      "push_null" >> {
        decorate(Vector(Line(0, ""), PushNull)) mustEqual Right(Root(Line(0, ""), PushNull))
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
      val result = decorate(Vector(Line(0, ""), PushString("/foo"), instructions.LoadLocal))
      result mustEqual Right(dag.LoadLocal(Line(0, ""), Root(Line(0, ""), PushString("/foo"))))
    }
    
    "parse out map1" in {
      val result = decorate(Vector(Line(0, ""), PushTrue, Map1(Neg)))
      result mustEqual Right(Operate(Line(0, ""), Neg, Root(Line(0, ""), PushTrue)))
    }
    
    "parse out reduce" in {
      val result = decorate(Vector(Line(0, ""), PushFalse, instructions.Reduce(BuiltInReduction(Reduction(Vector(), "count", 0x2000)))))
      result mustEqual Right(dag.Reduce(Line(0, ""), Reduction(Vector(), "count", 0x2000), Root(Line(0, ""), PushFalse)))
    }

    "parse out distinct" in {
      val result = decorate(Vector(Line(0, ""), PushNull, instructions.Distinct))
      result mustEqual Right(dag.Distinct(Line(0, ""), Root(Line(0, ""), PushNull)))
    }
    
    // TODO morphisms
    
    "parse a single-level split" in {
      val line = Line(0, "")
      
      val result = decorate(Vector(
        line,
        PushTrue,
        Dup,
        KeyPart(1),
        Swap(1),
        instructions.Group(2),
        instructions.Split,
        PushGroup(2),
        PushKey(1),
        IUnion,
        Merge))
        
      result must beLike {
        case Right(
          s @ dag.Split(`line`,
            dag.Group(2,
              Root(`line`, PushTrue),
              UnfixedSolution(1, Root(`line`, PushTrue))),
            IUI(`line`, true, 
              sg @ SplitGroup(`line`, 2, Vector()),
              sp @ SplitParam(`line`, 1)))) => {
              
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
        KeyPart(1),
        PushFalse,
        instructions.Group(2),
        instructions.Split,
        PushGroup(2),
        KeyPart(3),
        PushKey(1),
        instructions.Group(4),
        instructions.Split,
        PushGroup(4),
        PushKey(3),
        IUnion,
        Merge,
        Merge))
        
      result must beLike {
        case Right(
          s1 @ dag.Split(`line`,
            dag.Group(2, Root(`line`, PushFalse), UnfixedSolution(1, Root(`line`, PushTrue))),
            s2 @ dag.Split(`line`,
              dag.Group(4, sp1 @ SplitParam(`line`, 1), UnfixedSolution(3, sg1 @ SplitGroup(`line`, 2, Vector()))),
              IUI(`line`, true,
                sg2 @ SplitGroup(`line`, 4, Vector()),
                sp2 @ SplitParam(`line`, 3))))) => {
          
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
        KeyPart(1),
        PushFalse,
        instructions.Group(2),
        instructions.Split,
        PushGroup(2),
        PushKey(1),
        Map2Cross(Add),
        PushNum("42"),
        KeyPart(3),
        PushFalse,
        instructions.Group(4),
        instructions.Split,
        PushGroup(4),
        IUnion,
        Merge,
        Merge))
      
      result must beLike {
        case Right(
          s1 @ dag.Split(`line`,
            dag.Group(2, Root(`line`, PushFalse), UnfixedSolution(1, Root(`line`, PushTrue))),
            s2 @ dag.Split(`line`,
              dag.Group(4, Root(`line`, PushFalse), UnfixedSolution(3, Root(`line`, PushNum("42")))),
              IUI(`line`, true,
                Join(`line`, Add, CrossLeftSort,
                  sg1 @ SplitGroup(`line`, 2, Vector()),
                  sp1 @ SplitParam(`line`, 1)),
                sg2 @ SplitGroup(`line`, 4, Vector()))))) => {
          
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
          KeyPart(1),
          PushNum("3"),
          KeyPart(1),
          MergeBuckets(false),
          PushNum("2"),
          instructions.Group(3),
          instructions.Split,
          PushGroup(3),
          PushKey(1),
          IUnion,
          Merge))
          
        result must beLike {
          case Right(
            s @ dag.Split(`line`,
              dag.Group(3,
                Root(`line`, PushNum("2")),
                UnionBucketSpec(
                  UnfixedSolution(1, Root(`line`, PushNum("1"))),
                  UnfixedSolution(1, Root(`line`, PushNum("3"))))),
              IUI(`line`, true,
                sg @ SplitGroup(`line`, 3, Vector()),
                sp @ SplitParam(`line`, 1)))) => {
            
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
          KeyPart(1),
          PushNum("3"),
          KeyPart(1),
          MergeBuckets(true),
          PushNum("2"),
          instructions.Group(3),
          instructions.Split,
          PushGroup(3),
          PushKey(1),
          IUnion,
          Merge))
          
        result must beLike {
          case Right(
            s @ dag.Split(`line`,
              dag.Group(3,
                Root(`line`, PushNum("2")),
                IntersectBucketSpec(
                  UnfixedSolution(1, Root(`line`, PushNum("1"))),
                  UnfixedSolution(1, Root(`line`, PushNum("3"))))),
              IUI(`line`, true,
                sg @ SplitGroup(`line`, 3, Vector()),
                sp @ SplitParam(`line`, 1)))) => {
            
            sg.parent mustEqual s
            sp.parent mustEqual s
          }
        }
      }
    }
    
    // TODO union zip and zip with multiple keys
    "parse a split with zipped buckets" in {
      val line = Line(0, "")
      
      val result = decorate(Vector(
        line,
        PushNum("1"),
        KeyPart(1),
        PushNum("2"),
        instructions.Group(2),
        PushNum("3"),
        KeyPart(1),
        PushNum("4"),
        instructions.Group(3),
        MergeBuckets(true),
        instructions.Split,
        PushGroup(2),
        PushGroup(3),
        PushKey(1),
        IUnion,
        IUnion,
        Merge))
        
      result must beLike {
        case Right(
          s @ dag.Split(`line`,
            IntersectBucketSpec(
              dag.Group(2, Root(`line`, PushNum("2")), UnfixedSolution(1, Root(`line`, PushNum("1")))),
              dag.Group(3, Root(`line`, PushNum("4")), UnfixedSolution(1, Root(`line`, PushNum("3"))))),
            IUI(`line`, true,
              sg2 @ SplitGroup(`line`, 2, Vector()),
              IUI(`line`, true,
                sg1 @ SplitGroup(`line`, 3, Vector()),
                sp1 @ SplitParam(`line`, 1))))) => {
          
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
        KeyPart(1),
        PushNull,
        instructions.Group(2),
        instructions.Split,
        PushGroup(2),
        Map2Match(Add),
        Merge))
        
      result must beLike {
        case Right(
          s @ dag.Split(`line`,
            dag.Group(2, Root(`line`, PushNull), UnfixedSolution(1, Root(`line`, PushTrue))),
            Join(`line`, Add, IdentitySort,
              Root(`line`, PushNum("42")),
              sg @ SplitGroup(`line`, 2, Vector())))) => {
          
          sg.parent mustEqual s
        }
      }
    }
    
    "recognize a join instruction" in {
      "map2_match" >> {
        val line = Line(0, "")
        val result = decorate(Vector(line, PushTrue, PushFalse, Map2Match(Add)))
        result mustEqual Right(Join(line, Add, IdentitySort, Root(line, PushTrue), Root(line, PushFalse)))
      }
      
      "map2_cross" >> {
        val line = Line(0, "")
        val result = decorate(Vector(line, PushTrue, PushFalse, Map2Cross(Add)))
        result mustEqual Right(Join(line, Add, CrossLeftSort, Root(line, PushTrue), Root(line, PushFalse)))
      }
      
      "iunion" >> {
        val line = Line(0, "")
        val result = decorate(Vector(line, PushTrue, PushFalse, IUnion))
        result mustEqual Right(IUI(line, true, Root(line, PushTrue), Root(line, PushFalse)))
      }
      
      "iintersect" >> {
        val line = Line(0, "")
        val result = decorate(Vector(line, PushTrue, PushFalse, IIntersect))
        result mustEqual Right(IUI(line, false, Root(line, PushTrue), Root(line, PushFalse)))
      }      

      "set difference" >> {
        val line = Line(0, "")
        val result = decorate(Vector(line, PushTrue, PushFalse, SetDifference))
        result mustEqual Right(Diff(line, Root(line, PushTrue), Root(line, PushFalse)))
      }
    }
    
    "parse a filter with null predicate" in {
      val line = Line(0, "")
      val result = decorate(Vector(line, PushFalse, PushTrue, FilterMatch))
      result mustEqual Right(Filter(line, IdentitySort, Root(line, PushFalse), Root(line, PushTrue)))
    }
    
    "parse a filter_cross" in {
      val line = Line(0, "")
      val result = decorate(Vector(line, PushTrue, PushFalse, FilterCross))
      result mustEqual Right(Filter(line, CrossLeftSort, Root(line, PushTrue), Root(line, PushFalse)))
    }
    
    "parse a filter_crossl" in {
      val line = Line(0, "")
      val result = decorate(Vector(line, PushTrue, PushFalse, FilterCrossLeft))
      result mustEqual Right(Filter(line, CrossLeftSort, Root(line, PushTrue), Root(line, PushFalse)))
    }
    
    "parse a filter_crossr" in {
      val line = Line(0, "")
      val result = decorate(Vector(line, PushTrue, PushFalse, FilterCrossRight))
      result mustEqual Right(Filter(line, CrossRightSort, Root(line, PushTrue), Root(line, PushFalse)))
    }
    
    "continue processing beyond a filter" in {
      val line = Line(0, "")
      val result = decorate(Vector(line, PushFalse, PushTrue, FilterMatch, Map1(Neg)))
      result mustEqual Right(
        Operate(line, Neg,
          Filter(line, IdentitySort,
            Root(line, PushFalse),
            Root(line, PushTrue))))
    }
    
    "parse and factor a dup" in {
      {
        val line = Line(0, "")
        val result = decorate(Vector(line, PushTrue, Dup, IUnion))
        result mustEqual Right(IUI(line, true, Root(line, PushTrue), Root(line, PushTrue)))
      }
      
      {
        val line = Line(0, "")
        val result = decorate(Vector(line, PushNum("42"), Map1(Neg), Dup, IUnion))
        result mustEqual Right(IUI(line, true, Operate(line, Neg, Root(line, PushNum("42"))), Operate(line, Neg, Root(line, PushNum("42")))))
      }
    }
    
    "parse and factor a swap" in {
      "1" >> {
        val line = Line(0, "")
        val result = decorate(Vector(line, PushFalse, PushTrue, Swap(1), IUnion))
        result mustEqual Right(IUI(line, true, Root(line, PushTrue), Root(line, PushFalse)))
      }
      
      "3" >> {
        val line = Line(0, "")
        val result = decorate(Vector(line, PushTrue, PushString("foo"), PushFalse, PushNum("42"), Swap(3), IUnion, IUnion, IUnion))
        result mustEqual Right(
          IUI(line, true,
            Root(line, PushNum("42")),
            IUI(line, true,
              Root(line, PushString("foo")),
              IUI(line, true, Root(line, PushFalse), Root(line, PushTrue)))))
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
        val instr = instructions.Reduce(BuiltInReduction(Reduction(Vector(), "count", 0x2000)))
        decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }  

      "set-reduce" >> {     // similar to map1, only one underflow case!
        val instr = instructions.Distinct
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

      "set difference" >> {     // similar to map1, only one underflow case!
        val instr = SetDifference
        decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }
      
      "split" >> {     // similar to map1, only one underflow case!
        val instr = instructions.Split
        decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }
      
      // merge cannot stack underflow; curious, no?
      
      "filter_match" >> {
        {
          val instr = FilterMatch
          decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
        }
        
        {
          val instr = FilterMatch
          decorate(Vector(Line(0, ""), PushTrue, instr)) mustEqual Left(StackUnderflow(instr))
        }
        
        {
          val instr = FilterMatch
          decorate(Vector(Line(0, ""), PushTrue, PushTrue, Map2Match(Add), instr)) mustEqual Left(StackUnderflow(instr))
        }
      }
      
      "filter_cross" >> {
        {
          val instr = FilterCross
          decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
        }
        
        {
          val instr = FilterCross
          decorate(Vector(Line(0, ""), PushTrue, instr)) mustEqual Left(StackUnderflow(instr))
        }
        
        {
          val instr = FilterCross
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
        val instr = instructions.LoadLocal
        decorate(Vector(Line(0, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }
    }
    
    "reject multiple stack values at end" in {
      decorate(Vector(Line(0, ""), PushTrue, PushFalse)) mustEqual Left(MultipleStackValuesAtEnd)
      decorate(Vector(Line(0, ""), PushTrue, PushFalse, PushNum("42"))) mustEqual Left(MultipleStackValuesAtEnd)
      decorate(Vector(Line(0, ""), PushTrue, PushFalse, PushNum("42"), PushString("foo"))) mustEqual Left(MultipleStackValuesAtEnd)
    }
    
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
        KeyPart(1),
        PushFalse,
        instructions.Group(2),
        instructions.Split,
        PushKey(1),
        PushGroup(2),
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
        KeyPart(1),
        PushNum("12"),
        instructions.Group(2),
        instructions.Split,
        PushGroup(2),
        Swap(1),
        Swap(2),
        IUnion,
        Merge,
        IIntersect))
      
      lazy val split: dag.Split = dag.Split(line,
        dag.Group(2, Root(line, PushNum("12")), UnfixedSolution(1, Root(line, PushNum("42")))),
        IUI(line, true,
          SplitGroup(line, 2, Vector())(split),
          Root(line, PushTrue)))
      
      val expect = IUI(line, false, Root(line, PushFalse), split)
        
      result mustEqual Right(expect)
    }
    
    "reject unmatched merge" in {
      decorate(Vector(Line(0, ""), PushTrue, Merge)) mustEqual Left(UnmatchedMerge)
    }
    
    "reject split without corresponding merge" in {
      decorate(Vector(Line(0, ""),
        PushTrue,
        KeyPart(1),
        PushFalse,
        instructions.Group(2),
        instructions.Split)) mustEqual Left(UnmatchedSplit)
    }
    
    "reject split which increases the stack" in {
      val line = Line(0, "")
      
      val result = decorate(Vector(
        line,
        PushTrue,
        KeyPart(1),
        PushFalse,
        instructions.Group(2),
        instructions.Split,
        PushGroup(2),
        PushTrue,
        Merge))
        
      result mustEqual Left(MergeWithUnmatchedTails)
    }
  }
}
