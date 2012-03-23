package com.precog.bytecode

import org.scalacheck._
import Arbitrary.arbitrary
import Gen._

trait RandomLibrary extends Library {
  case class BIF1(namespace: Vector[String], name: String, opcode: Int) extends BuiltInFunc1
  case class BIF2(namespace: Vector[String], name: String, opcode: Int) extends BuiltInFunc2

  private lazy val genBuiltIn1 = for {
    op <- choose(0, 1000)
    n  <- identifier
    ns <- listOfN(2, identifier)
  } yield BIF1(Vector(ns: _*), n, op)

  private lazy val genBuiltIn2 = for {
    op <- choose(0, 1000)
    n  <- identifier
    ns <- listOfN(2, identifier)
  } yield BIF2(Vector(ns: _*), n, op)
    
  lazy val lib1 = containerOfN[Set, BIF1](30, genBuiltIn1).sample.get.map(op => (op.opcode, op)).toMap.values.toSet //make sure no duplicate opcodes
  lazy val lib2 = containerOfN[Set, BIF2](30, genBuiltIn2).sample.get.map(op => (op.opcode, op)).toMap.values.toSet //make sure no duplicate opcodes
  lazy val mathlib1 = containerOfN[Set, BIF1](30, genBuiltIn1).sample.get.map(op => (op.opcode, op)).toMap.values.toSet //make sure no duplicate opcodes
  lazy val mathlib2 = containerOfN[Set, BIF2](30, genBuiltIn2).sample.get.map(op => (op.opcode, op)).toMap.values.toSet //make sure no duplicate opcodes
}

trait InstructionGenerators extends Instructions with RandomLibrary {
  import instructions._

  implicit lazy val arbInstruction: Arbitrary[Instruction] = Arbitrary(genInstruction)
  
  implicit lazy val arbUnaryOp: Arbitrary[UnaryOperation] = Arbitrary(genUnaryOp)
  implicit lazy val arbBinaryOp: Arbitrary[BinaryOperation] = Arbitrary(genBinaryOp)
  
  implicit lazy val arbPredicate: Arbitrary[Predicate] = Arbitrary(genPredicate)
  implicit lazy val arbPredicateInstr: Arbitrary[PredicateInstr] = Arbitrary(genPredicateInstr)
  
  implicit lazy val arbReduction: Arbitrary[Reduction] = Arbitrary(genReduction)
  implicit lazy val arbSetReduction: Arbitrary[SetReduction] = Arbitrary(genSetReduction)
  
  implicit lazy val arbType: Arbitrary[Type] = Arbitrary(genType)
  
  private lazy val genInstruction: Gen[Instruction] = oneOf(
    genMap1,
    genMap2Match,
    genMap2Cross,
    genMap2CrossLeft,
    genMap2CrossRight,
    
    genReduce,
    genSetReduce,
    
    genVUnion,
    genVIntersect,
    
    genIUnion,
    genIIntersect,
    
    genBucket,
    genMergeBuckets,
    genZipBuckets,
    
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
  private lazy val genSetReduce = genSetReduction map SetReduce
  
  private lazy val genVUnion = VUnion
  private lazy val genVIntersect = VIntersect
  
  private lazy val genIUnion = IUnion
  private lazy val genIIntersect = IIntersect
  
  private lazy val genBucket = Bucket
  private lazy val genMergeBuckets = arbitrary[Boolean] map MergeBuckets
  private lazy val genZipBuckets = ZipBuckets
  
  private lazy val genSplit = for {
    n <- arbitrary[Short]
    k <- arbitrary[Short]
  } yield Split(n, k)
  
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
  private lazy val genDrop = Drop
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
    op  <- oneOf(lib1.toSeq)
    res <- oneOf(Comp, New, Neg, WrapArray, BuiltInFunction1Op(op))
  } yield res
  
  private lazy val genBinaryOp = for {
    op  <- oneOf(lib2.toSeq)
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

  private lazy val genReduction = oneOf(
    Count,
    GeometricMean,
    
    Mean,
    Median,
    Mode,
    
    Max,
    Min,
    
    StdDev,
    Sum,
    SumSq,
    Variance)

  private lazy val genSetReduction = Distinct
    
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
