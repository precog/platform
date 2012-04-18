package com.precog.yggdrasil

import com.precog.common._
import com.precog.common.util._
import blueeyes.json.JPath

import scala.collection.immutable.ListMap

import scalaz.Validation
import org.scalacheck.{Arbitrary, Gen}
import Gen._
import Arbitrary.arbitrary

trait ArbitraryProjectionDescriptor {
  def constructColumnDescriptor: Gen[ColumnDescriptor] = {
    def genPath: Gen[Path] = Gen.oneOf(Seq(Path("path1"),Path("path2"),Path("path3"),Path("path4"),Path("path5")))
    def genJPath: Gen[JPath] = Gen.oneOf(Seq(JPath("jpath1"),JPath("jpath2"),JPath("jpath3"),JPath("jpath4"),JPath("jpath5")))
    def genCType: Gen[CType] = Gen.oneOf(Seq(CBoolean, CInt, CLong, CFloat, CDouble, CDecimalArbitrary, CStringArbitrary))
    def genAuthorities: Gen[Authorities] = Gen.oneOf(Seq(Authorities(Set())))

    val genColumnDescriptor = for {
      path      <- genPath
      selector  <- genJPath
      valueType <- genCType
      ownership <- genAuthorities
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
  
  def genProjectionDescriptor: Gen[ProjectionDescriptor] = for {
    identities <- Gen.oneOf(1 to 5)
    columnDescriptors <- genListColDes
  } yield {
    ProjectionDescriptor(identities, columnDescriptors)
  }
}


// vim: set ts=4 sw=4 et:
