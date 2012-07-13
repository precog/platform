package com.precog.daze

import com.precog.bytecode.RandomLibrary

import org.specs2.mutable._

class TypeInferencerSpec extends Specification with DAG with RandomLibrary with TypeInferencer {
  import instructions._
  import dag._
  "type inference" should {
    "rewrite loads such that they will restrict the columns loaded" in {
      val line = Line(0, "")

      val input = Join(line, Map2Match(Add),
        Join(line, Map2Cross(DerefObject), 
          dag.LoadLocal(line, Root(line, PushString("/clicks"))),
          Root(line, PushString("time"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("/hom/heightWeight"))),
          Root(line, PushString("height"))))

      todo
    }
  }
}

// vim: set ts=4 sw=4 et:
