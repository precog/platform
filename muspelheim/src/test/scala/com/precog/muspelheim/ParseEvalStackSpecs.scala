/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog
package muspelheim

import common._
import common.kafka._

import daze._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import bytecode.JType

import yggdrasil._
import yggdrasil.actor._
import yggdrasil.serialization._
import yggdrasil.table._
import yggdrasil.util._
import muspelheim._

import org.specs2.mutable._

import akka.dispatch.Future
import akka.dispatch.Await
import akka.util.Duration

import java.io.File

import scalaz._
import scalaz.std.anyVal._
import scalaz.effect.IO

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import org.slf4j.LoggerFactory

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext

trait ParseEvalStackSpecs[M[+_]] extends Specification {
  type TestStack <: TestStackLike[M]
}

object TestStack {
  val testAPIKey = "dummyAPIKey"
  val testAccount = "dummyAccount"
}

trait ActorPlatformSpecs {
  implicit val actorSystem = ActorSystem("platformSpecsActorSystem")
  implicit val executor = ExecutionContext.defaultExecutionContext(actorSystem)
}

trait TestStackLike[M[+_]] extends ParseEvalStack[M]
    with MemoryDatasetConsumer[M]
    with IdSourceScannerModule 
    with EvalStackLike { self =>
  import TestStack._

  protected lazy val parseEvalLogger = LoggerFactory.getLogger("com.precog.muspelheim.ParseEvalStackSpecs")

  class ParseEvalStackSpecConfig extends BaseConfig with IdSourceConfig {
    parseEvalLogger.trace("Init yggConfig")
    val config = Configuration parse {
      Option(System.getProperty("precog.storage.root")) map { "precog.storage.root = " + _ } getOrElse { "" }
    }

    val sortWorkDir = scratchDir
    val memoizationBufferSize = sortBufferSize
    val memoizationWorkDir = scratchDir

    val flatMapTimeout = Duration(100, "seconds")
    val maxEvalDuration = Duration(5, "minutes")      // it's just unreasonable to run tests longer than this
    val clock = blueeyes.util.Clock.System

    val maxSliceSize = 10
    val smallSliceSize = 3

    val idSource = new FreshAtomicIdSource
  }

  def eval(str: String, debug: Boolean = false): Set[SValue] = evalE(str, debug) map { _._2 }

  def evalE(str: String, debug: Boolean = false): Set[SEvent] = {
    parseEvalLogger.debug("Beginning evaluation of query: " + str)

    val preForest = compile(str)
    val forest = preForest filter { _.errors filterNot isWarning isEmpty }

    assert(forest.size == 1 || preForest.forall(_.errors filterNot isWarning isEmpty))
    val tree = forest.head

    val Right(dag) = decorate(emit(tree))
    consumeEval(testAPIKey, dag, Path.Root) match {
      case Success(result) =>
        parseEvalLogger.debug("Evaluation complete for query: " + str)
        result
      case Failure(error) => throw error
    }
  }
}

// vim: set ts=4 sw=4 et:
