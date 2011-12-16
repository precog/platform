package com.reportgrid.storage.leveldb

import org.scalacheck.{Arbitrary,Gen}
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import Bijection._

class BijectionSpec extends Specification with ScalaCheck {
  // Disable the T => Descriptable conversion because that enables a conflictin "as" def
  implicit override def describe[T](t : => T) : Descriptible[T] = null

  override def defaultValues = super.defaultValues + (minTestsOk -> 1000)

  "Bijections" should {
    "Handle reversable serialization of Ints" in {
      check { (v : Int) => v.as[Array[Byte]].as[Int] must_== v }
    }

    "Handle reversable serialization of Longs" in {
      check { (v : Long) => v.as[Array[Byte]].as[Long] must_== v }
    }

    "Handle reversable serialization of BigDecimals" in {
      implicit lazy val arbBigDecimal: Arbitrary[BigDecimal] = {
        import java.math.MathContext._
        val bdGen = for {
          x <- Arbitrary.arbBigInt.arbitrary
          scale <- Gen.chooseNum(Int.MinValue + 1, Int.MaxValue)
        } yield {
          try { 
            BigDecimal(x, scale, java.math.MathContext.UNLIMITED)
          } catch {
            case e => println("Exception on %s, %d : %s".format(x, scale, e));  BigDecimal(0)
          }
        }
        Arbitrary(bdGen)
      }

      check { (v : BigDecimal) => 
        val underlying = v.bigDecimal
        underlying.as[Array[Byte]].as[java.math.BigDecimal] must_== underlying
      }
    }

    "Handle reversable serialization of BigIntegers" in {
      check { (v : BigInt) => 
        val underlying = v.bigInteger
        underlying.as[Array[Byte]].as[java.math.BigInteger] must_== underlying
      }
    }

    "Handle reversable serialization of List[Long]" in {
      implicit val genBigIntList = Gen.containerOf[List,Long](Arbitrary.arbitrary[Long])

      check { (ll : List[Long]) =>
        ll.toIterable.as[Array[Byte]].as[Iterable[Long]].toList must_== ll
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
