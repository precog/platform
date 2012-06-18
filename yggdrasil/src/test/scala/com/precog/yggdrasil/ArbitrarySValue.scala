package com.precog.yggdrasil

import blueeyes.json.JPath
import blueeyes.json.JPathField
import blueeyes.json.JPathIndex
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

trait CValueGenerators {
  type JSchema = Seq[(JPath, CType)]
  def schema(depth: Int): Gen[JSchema] = {
    if (depth <= 0) leafSchema
    else oneOf(1, 2, 3) flatMap {
      case 1 => objectSchema(depth, choose(1, 3))
      case 2 => arraySchema(depth, choose(1, 3))
      case 3 => leafSchema
    }
  }

  def objectSchema(depth: Int, sizeGen: Gen[Int]): Gen[JSchema] = {
    for {
      size <- sizeGen
      names <- listOfN(size, identifier)
      subschemas <- listOfN(size, schema(depth - 1))  
    } yield {
      for {
        (name, subschema) <- names zip subschemas
        (jpath, ctype)    <- subschema
      } yield {
        (JPathField(name) \ jpath, ctype)
      }
    }
  }

  def arraySchema(depth: Int, sizeGen: Gen[Int]): Gen[JSchema] = {
    for {
      size <- sizeGen
      subschemas <- listOfN(size, schema(depth - 1))
    } yield {
      for {
        (idx, subschema) <- (0 until size) zip subschemas
        (jpath, ctype)   <- subschema
      } yield {
        (JPathIndex(idx) \ jpath, ctype)
      }
    }
  }

  def leafSchema: Gen[JSchema] = ctype map { t => (JPath.Identity -> t) :: Nil }

  def ctype: Gen[CType] = oneOf(
    chooseNum(10, 1000).map(CStringFixed(_)),
    CStringArbitrary,
    CBoolean,
    CInt,
    CLong,
    CFloat,
    CDouble,
    CDecimalArbitrary,
    CNull,
    CEmptyObject,
    CEmptyArray
  )

  def jvalue(ctype: CType): Gen[JValue] = ctype match {
    case CStringFixed(width) => alphaStr map (JString(_))
    case CStringArbitrary => alphaStr map (JString(_))
    case CBoolean => arbitrary[Boolean] map (JBool(_))
    case CInt => arbitrary[Int] map (JInt(_))
    case CLong => arbitrary[Long] map (JInt(_))
    case CFloat => arbitrary[Float] map (JDouble(_))
    case CDouble => arbitrary[Double] map (JDouble(_))
    case CDecimalArbitrary => arbitrary[Double] map (JDouble(_))
    case CNull => JNull
    case CEmptyObject => JObject.empty 
    case CEmptyArray => JArray.empty
  }

  def jvalue(schema: Seq[(JPath, CType)]): Gen[JValue] = {
    schema.foldLeft(Gen.value[JValue](JNothing)) {
      case (gen, (jpath, ctype)) => 
        for {
          acc <- gen
          jv  <- jvalue(ctype)
        } yield {
          acc.unsafeInsert(jpath, jv)
        }
    }
  }

  def genEventColumns(jschema: JSchema): Gen[(Int, Stream[(Identities, Seq[(JPath, JValue)])])] = 
    for {
      idCount <- choose(1, 3) 
      data <- containerOf[Stream, (Identities, Seq[(JPath, JValue)])](
                for {
                  ids <- listOfN(idCount, posNum[Long])
                  values <- Gen.sequence[List, (JPath, JValue)](jschema map { case (jpath, ctype) => jvalue(ctype).map(jpath ->) })
                } yield {
                  (VectorCase(ids: _*), values)
                }
              )
    } yield {
      (idCount, data)
    }

  def assemble(parts: Seq[(JPath, JValue)]): JValue = {
    val result = parts.foldLeft[JValue](JNothing) { 
      case (acc, (selector, jv)) => acc.unsafeInsert(selector, jv) 
    }

    if (result != JNothing) result else sys.error("Cannot build object from " + parts)
  }
}

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
      size <- choose(0, 3)
      names <- listOfN(size, identifier)
      values <- listOfN(size, svalue(depth - 1))  
    } yield {
      SObject((names zip values).toMap)
    }
  }

  def sarray(depth: Int): Gen[SValue] = {
    for {
      size <- choose(0, 3)
      l <- listOfN(size, svalue(depth - 1))
    } yield SArray(Vector(l: _*))
  }


  def sleaf: Gen[SValue] = oneOf(
    alphaStr map (SString(_: String)),
    arbitrary[Boolean] map (SBoolean(_: Boolean)),
    arbitrary[Long]    map (l => SDecimal(BigDecimal(l))),
    arbitrary[Double]  map (d => SDecimal(BigDecimal(d))),
    //arbitrary[BigDecimal] map SDecimal, //scalacheck's BigDecimal gen will overflow at random
    value(SNull)
  )

  def sevent(idCount: Int, vdepth: Int): Gen[SEvent] = {
    for {
      ids <- listOfN(idCount, posNum[Long])
      value <- svalue(vdepth)
    } yield (VectorCase(ids: _*), value)
  }

  def chunk(size: Int, idCount: Int, vdepth: Int): Gen[Vector[SEvent]] = 
    listOfN(size, sevent(idCount, vdepth)) map { l => Vector(l: _*) }
}

case class LimitList[A](values: List[A])

object LimitList {
  def genLimitList[A: Gen](size: Int): Gen[LimitList[A]] = for {
    i <- choose(0, size)
    l <- listOfN(i, implicitly[Gen[A]])
  } yield LimitList(l)
}

trait ArbitrarySValue extends SValueGenerators {
  def genChunks(size: Int): Gen[LimitList[Vector[SEvent]]] = LimitList.genLimitList[Vector[SEvent]](size)

  implicit val listLongOrder = scalaz.std.list.listOrder[Long]

  implicit val SEventIdentityOrder: Order[SEvent] = Order[List[Long]].contramap((_: SEvent)._1.toList)
  implicit val SEventOrdering = SEventIdentityOrder.toScalaOrdering

  implicit val SEventChunkGen: Gen[Vector[SEvent]] = chunk(3, 3, 2)
  implicit val ArbitraryChunks = Arbitrary(genChunks(5))
}


// vim: set ts=4 sw=4 et:
