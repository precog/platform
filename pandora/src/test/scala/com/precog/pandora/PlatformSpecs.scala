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
package pandora

import daze._
import daze.util._

import pandora._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import yggdrasil._
import yggdrasil.shard._

import org.specs2.mutable._
  
import akka.dispatch.Await
import akka.util.Duration

import java.io.File

import scalaz._
import scalaz.effect.IO

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

class PlatformSpecs extends Specification
    with Compiler
    with LineErrors
    with ProvenanceChecker
    with Emitter
    with Evaluator
    with DatasetConsumers 
    with OperationsAPI
    with AkkaIngestServer 
    with YggdrasilEnumOpsComponent
    with LevelDBQueryComponent 
    with LevelDBMemoizationComponent {

  def loadConfig(dataDir: Option[String]): IO[BaseConfig with YggEnumOpsConfig with LevelDBQueryConfig with LevelDBMemoizationConfig] = IO {
    val rawConfig = dataDir map { "precog.storage.root = " + _ } getOrElse { "" }

    new BaseConfig with YggEnumOpsConfig with LevelDBQueryConfig with LevelDBMemoizationConfig {
      val config = Configuration.parse(rawConfig)  
      val flatMapTimeout = controlTimeout
      val projectionRetrievalTimeout = akka.util.Timeout(controlTimeout)
      val sortWorkDir = scratchDir
      val memoizationBufferSize = sortBufferSize
      val memoizationWorkDir = scratchDir
    }
  }
  
  val controlTimeout = Duration(30, "seconds")      // it's just unreasonable to run tests longer than this
  
  type YggConfig = YggEnumOpsConfig with LevelDBQueryConfig with LevelDBMemoizationConfig
  lazy val yggConfig = loadConfig(Option(System.getProperty("precog.storage.root"))).unsafePerformIO
  
  val maxEvalDuration = controlTimeout
  
  val Success(shardState) = YggState.restore(yggConfig.dataDir).unsafePerformIO

  object storage extends ActorYggShard {
    val yggState = shardState 
  }
  
  object ops extends Ops 
  
  object query extends QueryAPI 

  step {
    startup()
  }
  
  "the full stack" should {
    "count the campaigns dataset" >> {
      "<root>" >> {
        eval("count(dataset(//campaigns))") mustEqual Set(SDecimal(100))
      }
      
      "gender" >> {
        eval("count(dataset(//campaigns).gender)") mustEqual Set(SDecimal(100))
      }
      
      "platform" >> {
        eval("count(dataset(//campaigns).platform)") mustEqual Set(SDecimal(100))
      }
      
      "campaign" >> {
        eval("count(dataset(//campaigns).campaign)") mustEqual Set(SDecimal(100))
      }
      
      "cpm" >> {
        eval("count(dataset(//campaigns).cpm)") mustEqual Set(SDecimal(100))
      }
      
      "ageRange" >> {
        eval("count(dataset(//campaigns).ageRange)") mustEqual Set(SDecimal(100))
      }.pendingUntilFixed
    }
    
    "determine a histogram of genders on campaigns" in {
      val input = """
        | campaigns := dataset(//campaigns)
        | hist('gender) :=
        |   { gender: 'gender, num: count(campaigns.gender where campaigns.gender = 'gender) }
        | hist""".stripMargin
        
      eval(input) mustEqual Set(
        SObject(Map("gender" -> SString("female"), "num" -> SDecimal(55))),
        SObject(Map("gender" -> SString("male"), "num" -> SDecimal(45))))
    }
    
    "determine a histogram of genders on category" in {
      val input = """
        | campaigns := dataset(//campaigns)
        | organizations := dataset(//organizations)
        | 
        | hist('revenue, 'campaign) :=
        |   organizations' := organizations where organizations.revenue = 'revenue
        |   campaigns' := campaigns where campaigns.campaign = 'campaign
        |   organizations'' := organizations' where organizations'.campaign = 'campaign
        |   
        |   campaigns' :: organizations''
        |     { revenue: 'revenue, num: count(campaigns') }
        |   
        | hist""".stripMargin

      println("Waiting")
      Thread.sleep(30000)
      
      eval(input) mustEqual Set()   // TODO
    }
    
    /* commented out until we have memoization (MASSIVE time sink)
    "determine most isolated clicks in time" in {
      val input = """
        | clicks := dataset(//clicks)
        | 
        | spacings('time) :=
        |   click := clicks where clicks.time = 'time
        |   belowTime := max(clicks.time where clicks.time < click.time)
        |   aboveTime := min(clicks.time where clicks.time > click.time)
        |   
        |   {
        |     click: click,
        |     below: click.time - belowTime,
        |     above: aboveTime - click.time
        |   }
        |   
        | meanAbove := mean(spacings.above)
        | meanBelow := mean(spacings.below)
        | 
        | spacings.click where spacings.below > meanBelow | spacings.above > meanAbove""".stripMargin
        
      eval(input) mustEqual Set()   // TODO
    }
     */
  }
  
  step {
    shutdown()
  }
  
  
  def eval(str: String): Set[SValue] = evalE(str) map { _._2 }
  
  def evalE(str: String) = {
    val tree = compile(str)
    tree.errors must beEmpty
    val Right(dag) = decorate(emit(tree))
    consumeEval(dag)
  }
  
  def startup() {
    // start storage shard 
    Await.result(storage.start, controlTimeout)
  }
  
  def shutdown() {
    // stop storage shard
    Await.result(storage.stop, controlTimeout)
    
    actorSystem.shutdown()
  }
}
