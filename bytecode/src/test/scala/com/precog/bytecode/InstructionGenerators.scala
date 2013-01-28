package com.precog.bytecode

import org.scalacheck._
import Arbitrary.arbitrary
import Gen._

trait InstructionGenerators extends Instructions {
  import instructions._

  type Lib = RandomLibrary

  implicit lazy val arbInstruction: Arbitrary[Instruction] = Arbitrary(genInstruction)
  
  implicit lazy val arbUnaryOp: Arbitrary[UnaryOperation] = Arbitrary(genUnaryOp)
  implicit lazy val arbBinaryOp: Arbitrary[BinaryOperation] = Arbitrary(genBinaryOp)
  
  implicit lazy val arbReduction: Arbitrary[BuiltInReduction] = Arbitrary(genReduction)
  implicit lazy val arbMorphism1: Arbitrary[BuiltInMorphism1] = Arbitrary(genMorphism1)
  implicit lazy val arbMorphism2: Arbitrary[BuiltInMorphism2] = Arbitrary(genMorphism2)
  
  private lazy val genInstruction: Gen[Instruction] = oneOf(
    genMap1,
    genMap2Match,
    genMap2Cross,
    genMap2CrossLeft,
    genMap2CrossRight,
    
    genReduce,
    genMorph1,
    genMorph2,
    
    genAssert,
    
    genIUnion,
    genIIntersect,
    
    genGroup,
    genMergeBuckets,
    genKeyPart,
    genExtra,
    
    genSplit,
    genMerge,
    
    genFilterMatch,
    genFilterCross,
    genFilterCrossLeft,
    genFilterCrossRight,
    
    genDup,
    genDrop,
    genSwap,
    
    genLine,
    
    genLoadLocal,
    genDistinct,
    
    genPushString,
    genPushNum,
    genPushTrue,
    genPushFalse,
    genPushNull,
    genPushUndefined,
    genPushObject,
    genPushArray,
    
    genPushGroup,
    genPushKey)
    
  private lazy val genMap1 = genUnaryOp map Map1
  private lazy val genMap2Match = genBinaryOp map Map2Match
  private lazy val genMap2Cross = genBinaryOp map Map2Cross
  private lazy val genMap2CrossLeft = genBinaryOp map Map2CrossLeft
  private lazy val genMap2CrossRight = genBinaryOp map Map2CrossRight
  
  private lazy val genReduce = genReduction map Reduce
  private lazy val genMorph1 = genMorphism1 map Morph1
  private lazy val genMorph2 = genMorphism2 map Morph2
  
  private lazy val genAssert = Assert
  
  private lazy val genIUnion = IUnion
  private lazy val genIIntersect = IIntersect
  
  private lazy val genGroup = arbitrary[Int] map Group
  private lazy val genMergeBuckets = arbitrary[Boolean] map MergeBuckets
  private lazy val genKeyPart = arbitrary[Int] map KeyPart
  private lazy val genExtra = Extra
  
  private lazy val genSplit = Split
  private lazy val genMerge = Merge
  
  private lazy val genFilterMatch = FilterMatch
  private lazy val genFilterCross = FilterCross
  private lazy val genFilterCrossLeft = FilterCrossLeft
  private lazy val genFilterCrossRight = FilterCrossRight
  
  private lazy val genDup = Dup
  private lazy val genDrop = Drop
  private lazy val genSwap = arbitrary[Int] map Swap
  
  private lazy val genLine = for {
    line <- arbitrary[Int]
    col <- arbitrary[Int]
    text <- arbitrary[String]
  } yield Line(line, col, text)
  
  private lazy val genLoadLocal = LoadLocal
  private lazy val genDistinct = Distinct
  
  private lazy val genPushString = arbitrary[String] map PushString
  private lazy val genPushNum = arbitrary[String] map PushNum
  private lazy val genPushTrue = PushTrue
  private lazy val genPushFalse = PushFalse
  private lazy val genPushNull = PushNull 
  private lazy val genPushUndefined = PushUndefined
  private lazy val genPushObject = PushObject
  private lazy val genPushArray = PushArray
  
  private lazy val genPushGroup = arbitrary[Int] map PushGroup
  private lazy val genPushKey = arbitrary[Int] map PushKey
  
  private lazy val genUnaryOp = for {
    op <- oneOf(library.lib1.toSeq)
    res <- oneOf(Comp, New, Neg, WrapArray, BuiltInFunction1Op(op))
  } yield res
  
  private lazy val genBinaryOp = for {
    op <- oneOf(library.lib2.toSeq)
    res <- oneOf(
      Add,
      Sub,
      Mul,
      Div,
      
      Lt,
      LtEq,
      Gt,
      GtEq,
      
      Eq,
      NotEq,
      
      Or,
      And,
      
      WrapObject,
      
      JoinObject,
      JoinArray,
      
      ArraySwap,
      
      DerefObject,
      DerefArray,
    
      BuiltInFunction2Op(op))
  } yield res

  private lazy val genReduction = for {
    red <- oneOf(library.libReduction.toSeq)
    res <- BuiltInReduction(red)
  } yield res

  private lazy val genMorphism1 = for {
    m <- oneOf(library.libMorphism1.toSeq)
    res <- BuiltInMorphism1(m)
  } yield res

  private lazy val genMorphism2 = for {
    m <- oneOf(library.libMorphism2.toSeq)
    res <- BuiltInMorphism2(m)
  } yield res
}
