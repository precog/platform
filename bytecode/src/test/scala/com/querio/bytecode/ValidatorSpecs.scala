package com.querio.bytecode

import org.specs2.mutable._

object ValidatorSpecs extends Specification with Validator {
  import instructions._
  
  "operand stack validator" should {
    "reject a map1 on an empty stack" in {
      val inst = Map1(Comp)
      (validate(Vector(inst))) must beSome(StackUnderflow(inst))
    }
    
    "reject a map2 on a stack with one element" in {
      val inst = Map2Match(Add)
      validate(Vector(PushNum("42"), inst)) must beSome(StackUnderflow(inst))
    }
    
    "reject an instruction stream that results in the empty stack" in {
      validate(Vector()) must beSome(ExcessOperands)    // arguably confusing...
    }
    
    "reject an instruction stream that results in a stack with more than one operand" in {
      validate(Vector(PushNum("42"), PushNum("12"), Map2Match(Add), PushString("test"))) must beSome(ExcessOperands)
    }
    
    "accept an instruction stream that pushes and pops and ends in the right place" in {
      validate(Vector(PushNum("42"), PushNum("12"), Map2Match(Add))) must beNone
    }
    
    "reject an instruction stream that underflows in the middle" in {
      val inst = Map2Match(Sub)
      validate(Vector(PushNum("42"), PushNum("12"), Map2Match(Add), inst, PushString("test"))) must beSome(StackUnderflow(inst))
    }
    
    "reject an instruction stream that swaps at too great a depth" in {
      val inst = Swap(2)
      validate(Vector(PushNum("42"), PushNum("12"), inst, Map2Match(Add))) must beSome(StackUnderflow(inst))
    }
    
    "accept an instruction stream that swaps appropriately" in {
      val inst = Swap(1)
      validate(Vector(PushNum("42"), PushNum("12"), inst, Map2Match(Add))) must beNone
    }
    
    "reject a zero swap depth" in {
      val inst = Swap(0)
      validate(Vector(inst)) must beSome(NonPositiveSwapDepth(inst))
    }
    
    "reject a negative swap depth" in {
      val inst = Swap(-1)
      validate(Vector(inst)) must beSome(NonPositiveSwapDepth(inst))
    }
    
    "reject a negative filter_match depth" in {
      val inst = FilterMatch(-1, Some(Vector()))
      validate(Vector(inst)) must beSome(NegativeFilterDepth(inst))
    }
    
    "reject a negative filter_cross depth" in {
      val inst = FilterCross(-1, Some(Vector()))
      validate(Vector(inst)) must beSome(NegativeFilterDepth(inst))
    }
  }
  
  "predicate stack validator" should {
    "reject a predicate with non-matching depth" in {
      val inst = FilterCross(0, Some(Vector(Comp)))
      validate(Vector(PushNum("42"), PushNum("12"), inst)) must beSome(PredicateStackUnderflow(inst))
    }
    
    "accept a predicate with matching depth" in {
      val inst = FilterCross(1, Some(Vector(Comp)))
      validate(Vector(PushString("test"), PushNum("42"), PushNum("12"), inst)) must beNone
    }
    
    "reject a zero filter_match depth" in {
      val inst = FilterMatch(0, Some(Vector()))
      validate(Vector(PushNum("42"), PushNum("12"), inst)) must beSome(ExcessPredicateOperands(inst))
    }
    
    "reject a zero filter_cross depth" in {
      val inst = FilterCross(0, Some(Vector()))
      validate(Vector(PushNum("42"), PushNum("12"), inst)) must beSome(ExcessPredicateOperands(inst))
    }
    
    "reject a predicate that underflows" in {
      val inst = FilterCross(1, Some(Vector(Add)))
      validate(Vector(PushNum("42"), PushNum("12"), PushNum("6"), inst)) must beSome(PredicateStackUnderflow(inst))
    }
    
    // TODO we will reject this once we have stack *type* validation
    "accept an empty predicate that results in a depth of 1" in {
      val inst = FilterCross(1, Some(Vector()))
      validate(Vector(PushNum("42"), PushNum("12"), PushNum("6"), inst)) must beNone
    }
    
    "reject a predicate that results in a stack with more than one operand" in {
      val inst = FilterCross(2, Some(Vector()))
      validate(Vector(PushString("foo"), PushNum("42"), PushNum("12"), PushNum("6"), inst)) must beSome(ExcessPredicateOperands(inst))
    }
  }
}
