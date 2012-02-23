package com.precog.yggdrasil
package leveldb

import org.scalacheck.{Arbitrary, Gen}
import Gen._
import Arbitrary.arbitrary
import org.scalacheck.Prop
import org.scalacheck.Test.Params
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

import scala.collection.immutable.ListMap

import com.precog.yggdrasil._
import com.precog.yggdrasil.shard._
import com.precog.common._
import com.precog.analytics.Path
import com.precog.common.util._

import LevelDBByteProjectionSpec._

import blueeyes.json.JPath

import scalaz._


object LevelDBByteProjectionSpec {
  val cvInt = CInt(4)
  val cvLong = CLong(5)
  val cvString = CString("string")
  val cvBoolean = CBoolean(true)
  val cvFloat = CFloat(6)
  val cvDouble = CDouble(7)
  val cvDouble2 = CDouble(9)
  val cvNum = CNum(8)

  val colDesStringFixed: ColumnDescriptor = ColumnDescriptor(Path("path5"), JPath("key5"), SStringFixed(1), Ownership(Set()))
  val colDesStringArbitrary: ColumnDescriptor = ColumnDescriptor(Path("path6"), JPath("key6"), SStringArbitrary, Ownership(Set()))
  val colDesBoolean: ColumnDescriptor = ColumnDescriptor(Path("path0"), JPath("key0"), SBoolean, Ownership(Set()))
  val colDesInt: ColumnDescriptor = ColumnDescriptor(Path("path1"), JPath("key1"), SInt, Ownership(Set()))
  val colDesLong: ColumnDescriptor = ColumnDescriptor(Path("path2"), JPath("key2"), SLong, Ownership(Set()))
  val colDesFloat: ColumnDescriptor = ColumnDescriptor(Path("path3"), JPath("key3"), SFloat, Ownership(Set()))
  val colDesDouble: ColumnDescriptor = ColumnDescriptor(Path("path4"), JPath("key4"), SDouble, Ownership(Set()))
  val colDesDouble2: ColumnDescriptor = ColumnDescriptor(Path("path8"), JPath("key8"), SDouble, Ownership(Set()))  
  val colDesDecimal: ColumnDescriptor = ColumnDescriptor(Path("path7"), JPath("key7"), SDecimalArbitrary, Ownership(Set()))

  def byteProjectionInstance(indexedColumns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]) = { 
    ProjectionDescriptor(indexedColumns, sorting) map { d => 
      new LevelDBByteProjection {
        val descriptor: ProjectionDescriptor = d
      }
    }
  }

  def byteProjectionInstance2(desc: ProjectionDescriptor) = {
    desc map { desc =>
      new LevelDBByteProjection {
        val descriptor: ProjectionDescriptor = desc
      }
    }
  }

  def identityFunction(identities: Identities, values: Seq[CValue]): (Identities, Seq[CValue]) = (identities, values)

  def constructByteProjection(columns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]) = {
    byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }
  }
  
  def constructByteProjection2(desc: ProjectionDescriptor) = {
    byteProjectionInstance2(desc) ///||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }
  }

  def constructIds(uniqueIndices: Int): Vector[Long] = {  
    Gen.listOfN(uniqueIndices, Arbitrary.arbitrary[Long]).sample.get.foldLeft(Vector.empty: Vector[Long])((v, id) => v :+ id)
  }

  def constructValues(listOfColDes: List[ColumnDescriptor]): Seq[CValue] = {
    val listOfTypes = listOfColDes.foldLeft(List.empty: List[ColumnType])((l,col) => l :+ col.valueType)
    listOfTypes.foldLeft(Seq.empty: Seq[CValue]) {
      case (seq, SInt)  => seq :+ cvInt
      case (seq, SFloat) => seq :+ cvFloat
      case (seq, SBoolean) => seq :+ cvBoolean
      case (seq, SDouble) => seq :+ cvDouble
      case (seq, SLong) => seq :+ cvLong
      case (seq, SDecimalArbitrary) => seq :+ cvNum
      case (seq, SStringArbitrary) => seq :+ cvString
    }
  }
}

class LevelDBByteProjectionSpec extends Specification with ScalaCheck {
  "a byte projection generated from sample data" should {
    "return the arguments of project, when unproject is applied to project" in {
      val routingTable: RoutingTable = SingleColumnProjectionRoutingTable
      val dataPath = Path("/test")
      implicit val (sampleData, _) = DistributedSampleSet.sample(5, 0)
      //println(sampleData)


      val projDes: List[ProjectionDescriptor] = {
        sampleData.zipWithIndex.foldLeft(List.empty[ProjectionDescriptor]) {
          case (acc, (jobj, i)) => routingTable.route(EventMessage(EventId(0, i), Event(dataPath, "", jobj, Map()))).foldLeft(acc) {
            case (acc, ProjectionData(descriptor, identities, values, _)) =>
              acc :+ descriptor
          }
        }
      }
      val byteProj = projDes.foldLeft(List.empty[LevelDBByteProjection]) {
        case (l, des) => l :+ byteProjectionInstance2(des).sample.get
      }
      
      projDes.zip(byteProj).map { 
        case (des, byte) =>
          val identities: Vector[Long] = constructIds(des.indexedColumns.values.toSet.size)
          val values: Seq[CValue] = constructValues(des.columns)
          val (projectedIds, projectedValues) = byte.project(VectorCase.fromSeq(identities), values)

          byte.unproject(projectedIds, projectedValues)(identityFunction) must_== (identities, values)
      }
    }
  }

  /* "a scalacheck-generated byte projection" should {
    "return the arguments of project, when unproject is applied to project" in {
      import Gen._

      def constructColumnDescriptor: Gen[ColumnDescriptor] = {
        def genPath: Gen[Path] = Gen.oneOf(Seq(Path("path1"),Path("path2"),Path("path3"),Path("path4"),Path("path5")))
        def genJPath: Gen[JPath] = Gen.oneOf(Seq(JPath("jpath1"),JPath("jpath2"),JPath("jpath3"),JPath("jpath4"),JPath("jpath5")))
        def genColumnType: Gen[ColumnType] = Gen.oneOf(Seq(SInt,SFloat,SBoolean,SDouble,SLong,SDecimalArbitrary,SStringArbitrary))
        def genOwnership: Gen[Ownership] = Gen.oneOf(Seq(Ownership(Set())))
  
        val genColumnDescriptor = for {
          path      <- genPath
          selector  <- genJPath
          valueType <- genColumnType
          ownership <- genOwnership
        } yield ColumnDescriptor(path, selector, valueType, ownership)
  
        genColumnDescriptor
      }

      def genListColDes: Gen[List[ColumnDescriptor]] = for {
        number <- Gen.oneOf(1 to 30)
        listColDes <- Gen.listOfN(number, constructColumnDescriptor).map(_.groupBy(c => (c.path, c.selector)).filter {
          case (_, value) => value.size == 1 
         }.foldLeft(List.empty[ColumnDescriptor]) {
           case (list, (_, colDes)) => list ++ colDes
         })
      } yield {
        listColDes
      }
      
      def genProjectionDescriptor: Gen[Validation[String, ProjectionDescriptor]] = for {
        listColDes        <- genListColDes
        indexColumnCount  <- Gen.oneOf(1 to listColDes.size)
        baseList          <- Gen.value((0 to indexColumnCount - 1).toList)
        list              <- Gen.listOfN(listColDes.size - indexColumnCount, Gen.oneOf(0 to indexColumnCount - 1))
        sortBy            <- Gen.listOfN(listColDes.size, Gen.oneOf(Seq(ByValue, ById, ByValueThenId)))
      } yield {
        val listOfIndices = baseList ++ list
        val columns = listColDes.zip(listOfIndices).foldLeft(ListMap.empty: ListMap[ColumnDescriptor, Int]) { 
          case (lm, (colDes, index)) => lm + ((colDes, index))
        }
        val sorting = listColDes.zip(sortBy)

        ProjectionDescriptor(columns, sorting)
      }

      implicit val arbProjection: Arbitrary[LevelDBByteProjection] = Arbitrary(
        genProjectionDescriptor flatMap {
          case Success(desc) => 
            Gen.value(
              new LevelDBByteProjection {
                val descriptor: ProjectionDescriptor = desc
              }
            )

          case Failure(reason) => 
            //println(reason)
            Gen.fail
        }
      )
          
      check { (byteProjection: LevelDBByteProjection) =>
        val identities: Vector[Long] = constructIds(byteProjection.descriptor.indexedColumns.values.toList.distinct.size)
        val values: Seq[CValue] = constructValues(byteProjection.descriptor.columns)

        val (projectedIds, projectedValues) = byteProjection.project(identities, values)

        byteProjection.unproject(projectedIds, projectedValues)(identityFunction) must_== (identities, values) 
      }.set(maxDiscarded -> 500, minTestsOk -> 200)
    }
  } */

  "a byte projection" should {
    "project to the expected key format when a single id matches two sortings, ById and ByValue (test1)" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesLong -> 0, colDesBoolean -> 0, colDesFloat -> 1)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesLong, ById),(colDesBoolean, ByValue),(colDesFloat, ByValueThenId))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val expectedKey: Array[Byte] = Array(0,0,0,0,0,0,0,1,1,64,-64,0,0,0,0,0,0,0,0,0,2)
      val expectedValue: Array[Byte] = Array(0,0,0,0,0,0,0,5)

      val (key, value) = byteProjection.project(VectorCase(1L,2L), Seq(cvLong, cvBoolean, cvFloat))
      key must_== expectedKey
      value must_== expectedValue
    }

    "project to the expected key format another when an id is only sorted ByValue (test2)" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesInt -> 0, colDesDouble -> 1, colDesBoolean -> 1)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue),(colDesDouble, ByValue),(colDesBoolean, ByValue))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val expectedKey: Array[Byte] = Array(0,0,0,4,64,28,0,0,0,0,0,0,1,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,2)
      val expectedValue: Array[Byte] = Array()

      val (key, value) = byteProjection.project(VectorCase(1L,2L), Seq(cvInt, cvDouble, cvBoolean))
      key must_== expectedKey
      value must_== expectedValue
    } 

    "project to the expected key format (case with five columns) (test3)" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesInt -> 0, colDesLong -> 0, colDesFloat -> 1, colDesDouble -> 2, colDesBoolean -> 2)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue),(colDesLong, ByValue), (colDesFloat, ById), (colDesDouble, ByValue), (colDesBoolean, ById)) 
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val expectedKey: Array[Byte] = Array(0,0,0,4,0,0,0,0,0,0,0,5,0,0,0,0,0,0,0,2,64,28,0,0,0,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,0,0,0,1)
      val expectedValue: Array[Byte] = Array(64,-64,0,0,1)

      val (key, value) = byteProjection.project(VectorCase(1L,2L,3L), Seq(cvInt, cvLong, cvFloat, cvDouble, cvBoolean))
      key must_== expectedKey
      value must_== expectedValue
    }
  }

  "when applied to the project function, the unproject function" should {
    "return the arguments of the project function (test1)" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesLong -> 0, colDesBoolean -> 0, colDesFloat -> 1)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesLong, ById),(colDesBoolean, ByValue),(colDesFloat, ByValueThenId))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val (projectedIds, projectedValues) = byteProjection.project(VectorCase(1L,2L), Seq(cvLong, cvBoolean, cvFloat))

      byteProjection.unproject(projectedIds, projectedValues)(identityFunction) must_== (VectorCase(1L,2L), Seq(cvLong, cvBoolean, cvFloat)) 
    }

    "return the arguments of the project function (test2)" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesInt -> 0, colDesDouble -> 1, colDesBoolean -> 1)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue),(colDesDouble, ByValue),(colDesBoolean, ByValue))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val (projectedIds, projectedValues) = byteProjection.project(VectorCase(1L,2L), Seq(cvInt, cvDouble, cvBoolean))

      byteProjection.unproject(projectedIds, projectedValues)(identityFunction) must_== (VectorCase(1L,2L), Seq(cvInt, cvDouble, cvBoolean)) 
    }

    "return the arguments of the project function (test3)" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesFloat -> 0, colDesInt -> 1, colDesDouble -> 1, colDesBoolean -> 2, colDesDouble2 -> 2)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesFloat, ById),(colDesInt, ByValue), (colDesDouble, ById), (colDesBoolean, ByValue), (colDesDouble2, ById)) 
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }
 
      val (projectedIds, projectedValues) = byteProjection.project(VectorCase(1L,2L,3L), Seq(cvFloat, cvInt, cvDouble, cvBoolean, cvDouble2))

      byteProjection.unproject(projectedIds, projectedValues)(identityFunction) must_== (VectorCase(1L,2L,3L), Seq(cvFloat, cvInt, cvDouble, cvBoolean, cvDouble2)) 
    }

    
  }

}







