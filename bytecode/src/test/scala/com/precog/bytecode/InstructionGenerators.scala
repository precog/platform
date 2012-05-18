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
package com.precog.bytecode

import org.scalacheck._
import Arbitrary.arbitrary
import Gen._

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
    
    genPushString,
    genPushNum,
    genPushTrue,
    genPushFalse,
    genPushNull,
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
  private lazy val genSetReduce = genSetReduction map SetReduce
  
  private lazy val genVUnion = VUnion
  private lazy val genVIntersect = VIntersect
  
  private lazy val genIUnion = IUnion
  private lazy val genIIntersect = IIntersect
  
  private lazy val genGroup = arbitrary[Int] map Group
  private lazy val genMergeBuckets = arbitrary[Boolean] map MergeBuckets
  private lazy val genKeyPart = arbitrary[Int] map KeyPart
  private lazy val genExtra = Extra
  
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
  private lazy val genPushNull = PushNull 
  private lazy val genPushObject = PushObject
  private lazy val genPushArray = PushArray
  
  private lazy val genPushGroup = arbitrary[Int] map PushGroup
  private lazy val genPushKey = arbitrary[Int] map PushKey
  
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
