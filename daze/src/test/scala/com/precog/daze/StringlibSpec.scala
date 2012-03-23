package com.precog.daze

import org.specs2.mutable._

import com.precog.yggdrasil._

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.list._
import Iteratee._

import com.precog.common.VectorCase
import com.precog.util.IdGen

class StringlibSpec extends Specification
  with Evaluator
  with StubOperationsAPI 
  with TestConfigComponent 
  with DiskMemoizationComponent 
  with Stringlib { self =>
    

  import Function._
  
  import dag._
  import instructions._

  object ops extends Ops 
  
  val testUID = "testUID"

  def testEval = consumeEval(testUID, _: DepGraph) match {
    case Success(results) => results
    case Failure(error) => throw error
  }

  "all string functions" should {
    "validate input" in todo
    "return failing validations for bad input" in todo
  }

  "the appropriate string function" should {
    "determine length" in todo
  }
}
