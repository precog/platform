package com.precog
package muspelheim

import yggdrasil._
import blueeyes.json._
import com.precog.common._
import org.specs2.mutable._

trait EvalStackSpecs extends Specification {
  type TestStack <: EvalStackLike
  val stack: TestStack
}

trait EvalStackLike {
  type IdType

  def eval(str: String, debug: Boolean = false): Set[SValue]
  def evalE(str: String, debug: Boolean = false): Set[(Vector[IdType], SValue)]
}
