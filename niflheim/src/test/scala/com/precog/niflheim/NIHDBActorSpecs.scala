package com.precog.niflheim

import com.precog.common.Path

import org.scalacheck.{Arbitrary, Gen}

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

class NIHDBActorSpecs extends Specification with ScalaCheck {
  import Gen._

  val hasSuffixGen: Gen[String] = resultOf[String, String] {
    case prefix => prefix + NIHDBActor.escapeSuffix
  }(Arbitrary(alphaStr))

  val componentGen: Gen[String] = Gen.oneOf(Gen.value(NIHDBActor.cookedSubdir), Gen.value(NIHDBActor.rawSubdir), hasSuffixGen, alphaStr)

  implicit val pathGen: Arbitrary[Path] =
    Arbitrary(for {
      componentCount <- chooseNum(0, 200)
      components     <- listOfN(componentCount, componentGen)
    } yield Path(components))

  "NIHDBActor path escaping" should {
    import NIHDBActor._

    "Handle arbitrary paths with components needing escaping" in check {
      (p: Path) => unescapePath(escapePath(p)) mustEqual p
    }
  }
}
