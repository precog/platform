package com.precog.bytecode

import org.scalacheck._

trait InstructionGenerators extends Instructions {
  import instructions._
  
  import Arbitrary.arbitrary
  import Gen._
  
  implicit lazy val arbInstruction: Arbitrary[Instruction] = Arbitrary(genInstruction)
  
  implicit lazy val arbUnaryOp: Arbitrary[UnaryOperation] = Arbitrary(genUnaryOp)
  implicit lazy val arbBinaryOp: Arbitrary[BinaryOperation] = Arbitrary(genBinaryOp)
  
  implicit lazy val arbPredicate: Arbitrary[Predicate] = Arbitrary(genPredicate)
  implicit lazy val arbPredicateInstr: Arbitrary[PredicateInstr] = Arbitrary(genPredicateInstr)
  
  implicit lazy val arbReduction: Arbitrary[Reduction] = Arbitrary(genReduction)
  
  implicit lazy val arbType: Arbitrary[Type] = Arbitrary(genType)
  
  private lazy val genInstruction: Gen[Instruction] = oneOf(
    genMap1,
    genMap2Match,
    genMap2Cross,
    genMap2CrossLeft,
    genMap2CrossRight,
    
    genReduce,
    
    genVUnion,
    genVIntersect,
    
    genIUnion,
    genIIntersect,
    
    genSplit,
    genMerge,
    
    genFilterMatch,
    genFilterCross,
    genFilterCrossLeft,
    genFilterCrossRight,
    
    genDup,
    genSwap,
    
    genLine,
    
    genLoadLocal,
    
    genPushString,
    genPushNum,
    genPushTrue,
    genPushFalse,
    genPushObject,
    genPushArray)
    
  private lazy val genMap1 = genUnaryOp map Map1
  private lazy val genMap2Match = genBinaryOp map Map2Match
  private lazy val genMap2Cross = genBinaryOp map Map2Cross
  private lazy val genMap2CrossLeft = genBinaryOp map Map2CrossLeft
  private lazy val genMap2CrossRight = genBinaryOp map Map2CrossRight
  
  private lazy val genReduce = genReduction map Reduce
  
  private lazy val genVUnion = VUnion
  private lazy val genVIntersect = VIntersect
  
  private lazy val genIUnion = IUnion
  private lazy val genIIntersect = IIntersect
  
  private lazy val genSplit = Split
  private lazy val genMerge = Merge
  
  private lazy val genFilterMatch = for {
    depth <- arbitrary[Short]
    pred <- genPredicate
    optPred <- oneOf(Some(pred), None)
  } yield FilterMatch(depth, optPred)
  
  private lazy val genFilterCross = for {
    depth <- arbitrary[Short]
    pred <- genPredicate
    optPred <- oneOf(Some(pred), None)
  } yield FilterCross(depth, optPred)
  
  private lazy val genFilterCrossLeft = genFilterCross map {
    case FilterCross(depth, pred) => FilterCrossLeft(depth, pred)
  }
  
  private lazy val genFilterCrossRight = genFilterCross map {
    case FilterCross(depth, pred) => FilterCrossRight(depth, pred)
  }
  
  private lazy val genDup = Dup
  private lazy val genSwap = arbitrary[Int] map Swap
  
  private lazy val genLine = for {
    num <- arbitrary[Int]
    text <- arbitrary[String]
  } yield Line(num, text)
  
  private lazy val genLoadLocal = genType map LoadLocal
  
  private lazy val genPushString = arbitrary[String] map PushString
  private lazy val genPushNum = arbitrary[String] map PushNum
  private lazy val genPushTrue = PushTrue
  private lazy val genPushFalse = PushFalse
  private lazy val genPushObject = PushObject
  private lazy val genPushArray = PushArray
  
  private lazy val genUnaryOp = for {
    op <- genBuiltIn1
    res <- oneOf(Comp, New, Neg, WrapArray, BuiltInFunction1(op))
  } yield res
  
  private lazy val genBinaryOp = oneOf(
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
    DerefArray)

  private lazy val genBuiltIn1 = oneOf(
    Date,
    Year,
    QuarterOfYear)

  private lazy val genBuiltIn2 = oneOf(
    ChangeTimeZone
  )
    
  private lazy val genReduction = oneOf(
    Count,
    
    Mean,
    Median,
    Mode,
    
    Max,
    Min,
    
    StdDev,
    Sum)
    
  private lazy val genPredicate = listOf(genPredicateInstr) map { xs => Vector(xs: _*) }
  
  private lazy val genPredicateInstr = oneOf(
    Add,
    Sub,
    Mul,
    Div,
    
    Or,
    And,
    
    Comp,
    Neg,
    
    DerefObject,
    DerefArray,
    
    Range)
    
  private lazy val genType = Het
}
