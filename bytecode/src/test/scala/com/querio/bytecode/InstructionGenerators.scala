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
    genMap2CrossLeft,
    genMap2CrossRight,
    genMap2Cross,
    
    genReduce,
    
    genVUnion,
    genVIntersect,
    
    genIUnion,
    genIIntersect,
    
    genSplit,
    genMerge,
    
    genFilterMatch,
    genFilterCross,
    
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
  private lazy val genMap2CrossLeft = genBinaryOp map Map2CrossLeft
  private lazy val genMap2CrossRight = genBinaryOp map Map2CrossRight
  private lazy val genMap2Cross = genBinaryOp map Map2Cross
  
  private lazy val genReduce = genReduction map Reduce
  
  private lazy val genVUnion = VUnion
  private lazy val genVIntersect = VIntersect
  
  private lazy val genIUnion = IUnion
  private lazy val genIIntersect = IIntersect
  
  private lazy val genSplit = Split
  private lazy val genMerge = Merge
  
  private lazy val genFilterMatch = for {
    depth <- arbitrary[Int]
    pred <- genPredicate
  } yield FilterMatch(depth, pred)
  
  private lazy val genFilterCross = for {
    depth <- arbitrary[Int]
    pred <- genPredicate
  } yield FilterCross(depth, pred)
  
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
  
  private lazy val genUnaryOp = oneOf(Comp, Neg, WrapArray)
  
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
    
    DerefObject,
    DerefArray)
    
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
