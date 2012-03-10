package com.precog.yggdrasil

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import com.precog.common.VectorCase

import org.specs2.mutable.Specification
import org.scalacheck.Gen
import org.scalacheck.Arbitrary
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary._

import scalaz.Order
import scalaz.std.list._
import scalaz.std.anyVal._

trait SValueGenerators {
  def svalue(depth: Int): Gen[SValue] = {
    if (depth <= 0) sleaf 
    else oneOf(1, 2, 3) flatMap { //it's much faster to lazily compute the subtrees
      case 1 => sobject(depth)
      case 2 => sarray(depth)
      case 3 => sleaf
    }
  }

  def sobject(depth: Int): Gen[SValue] = {
    for {
      size <- choose(1, 3)
      names <- listOfN(size, identifier)
      values <- listOfN(size, svalue(depth - 1))  
    } yield {
      SObject((names zip values).toMap)
    }
  }

  def sarray(depth: Int): Gen[SValue] = {
    for {
      size <- choose(1, 3)
      l <- listOfN(size, svalue(depth - 1))
    } yield SArray(Vector(l: _*))
  }


  def sleaf: Gen[SValue] = oneOf(
    alphaStr map SString,
    arbitrary[Boolean] map SBoolean,
    arbitrary[Long] map SLong,
    arbitrary[Double] map SDouble,
    //arbitrary[BigDecimal] map SDecimal, //scalacheck's BigDecimal gen will overflow at random
    value(SNull)
  )

  def sevent(idCount: Int, vdepth: Int): Gen[SEvent] = {
    for {
      ids <- listOfN(idCount, arbitrary[Long])
      value <- svalue(vdepth)
    } yield (VectorCase(ids: _*), value)
  }

  def chunk(size: Int, idCount: Int, vdepth: Int): Gen[Vector[SEvent]] = 
    listOfN(size, sevent(idCount, vdepth)) map { l => Vector(l: _*) }
}

object ArbitrarySValue extends SValueGenerators {
  case class LimitList[A](values: List[A])

  implicit def arbLimitList[A: Gen](size: Int): Arbitrary[LimitList[A]] = Arbitrary {
    for {
      i <- choose(0, size)
      l <- listOfN(i, implicitly[Gen[A]])
    } yield LimitList(l)
  }

  implicit val SEventIdentityOrder: Order[SEvent] = Order[List[Long]].contramap((_: SEvent)._1.toList)
  implicit val SEventOrdering = SEventIdentityOrder.toScalaOrdering

  implicit val SEventGen: Gen[Vector[SEvent]] = chunk(3, 3, 2)
  implicit val ArbitraryChunks = arbLimitList[Vector[SEvent]](5)
}


// vim: set ts=4 sw=4 et:
