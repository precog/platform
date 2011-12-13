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
package reportgrid.storage.leveldb

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
