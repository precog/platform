package com.precog.bytecode

import org.scalacheck._

trait UtilGenerators {
  import Arbitrary.arbitrary
  import Gen._
  
  implicit def arbVector[A : Arbitrary]: Arbitrary[Vector[A]] =
    Arbitrary(genVector[A])
  
  private def genVector[A : Arbitrary]: Gen[Vector[A]] = 
    listOf(implicitly[Arbitrary[A]].arbitrary) map { xs => Vector(xs: _*) }
}
