package com.precog
package muspelheim

import yggdrasil._
import blueeyes.json._
import com.precog.common._
import org.specs2.mutable._

trait EvalStackSpecs extends Specification {
  def eval(str: String, debug: Boolean = false): Set[SValue]
  def evalE(str: String, debug: Boolean = false): Set[(Vector[Long], SValue)]
}

// vim: set ts=4 sw=4 et:
