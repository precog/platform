package com.precog.yggdrasil

import com.precog.common._
import com.precog.common.util._

import blueeyes.json.JPath

import scala.collection.immutable.ListMap

import scalaz.Validation
import org.scalacheck.{Arbitrary, Gen}
import Gen._
import Arbitrary.arbitrary

//trait ArbitraryProjectionDescriptor {
//  def constructColumnRef: Gen[ColumnRef] = {
//    def genPath: Gen[Path] = Gen.oneOf(Seq(Path("path1"),Path("path2"),Path("path3"),Path("path4"),Path("path5")))
//    def genJPath: Gen[JPath] = Gen.oneOf(Seq(JPath("jpath1"),JPath("jpath2"),JPath("jpath3"),JPath("jpath4"),JPath("jpath5")))
//    def genCType: Gen[CType] = Gen.oneOf(Seq(CBoolean, CLong, CDouble, CNum, CString))
//    def genAuthorities: Gen[Authorities] = Gen.oneOf(Seq(Authorities(Set())))
//
//    val genColumnRef = for {
//      path      <- genPath
//      selector  <- genJPath // TODO genCPath
//      valueType <- genCType
//      ownership <- genAuthorities
//    } yield ColumnRef(path, CPath(selector), valueType, ownership)
//
//    genColumnRef
//  }
//
//  def genListColDes: Gen[List[ColumnRef]] = for {
//    number <- Gen.oneOf(1 to 30)
//    listColDes <- Gen.listOfN(number, constructColumnRef).map(_.groupBy(c => (c.path, c.selector)).filter {
//      case (_, value) => value.size == 1 
//     }.foldLeft(List.empty[ColumnRef]) {
//       case (list, (_, colDes)) => list ++ colDes
//     })
//  } yield {
//    listColDes
//  }
//  
//  def genProjectionDescriptor: Gen[ProjectionDescriptor] = for {
//    identities <- Gen.oneOf(1 to 5)
//    columnDescriptors <- genListColDes
//  } yield {
//    ProjectionDescriptor(identities, columnDescriptors)
//  }
//}


// vim: set ts=4 sw=4 et:
