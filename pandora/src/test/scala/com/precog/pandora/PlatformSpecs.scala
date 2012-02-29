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

import common.VectorCase
import common.kafka._

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
    with DiskMemoizationComponent {

  lazy val controlTimeout = Duration(30, "seconds")      // it's just unreasonable to run tests longer than this
  trait YggConfig extends BaseConfig with YggEnumOpsConfig with LevelDBQueryConfig with DiskMemoizationConfig with DatasetConsumersConfig 

  object yggConfig extends YggConfig {
    lazy val config = Configuration parse {
      Option(System.getProperty("precog.storage.root")) map { "precog.storage.root = " + _ } getOrElse { "" }
    }

    lazy val flatMapTimeout = controlTimeout
    lazy val projectionRetrievalTimeout = akka.util.Timeout(controlTimeout)
    lazy val sortWorkDir = scratchDir
    lazy val chunkSerialization = SimpleProjectionSerialization
    lazy val memoizationBufferSize = sortBufferSize
    lazy val memoizationWorkDir = scratchDir
    lazy val maxEvalDuration = controlTimeout
  }

  lazy val Success(shardState) = YggState.restore(yggConfig.dataDir).unsafePerformIO

  type Storage = ActorYggShard
  object storage extends ActorYggShard {
    lazy val yggState = shardState 
    lazy val yggCheckpoints = new TestYggCheckpoints
    lazy val batchConsumer = BatchConsumer.NullBatchConsumer
  }
  
  object ops extends Ops 
  
  object query extends QueryAPI 

  step {
    startup()
  }
  
  "the full stack" should {
    "count a filtered clicks dataset" in {
      val input = """
        | clicks := load(//clicks)
        | count(clicks where clicks.time > 0)""".stripMargin
        
      eval(input) mustEqual Set(SDecimal(100))
    }
    
    "count the campaigns dataset" >> {
      "<root>" >> {
        eval("count(load(//campaigns))") mustEqual Set(SDecimal(100))
      }
      
      "gender" >> {
        eval("count(load(//campaigns).gender)") mustEqual Set(SDecimal(100))
      }
      
      "platform" >> {
        eval("count(load(//campaigns).platform)") mustEqual Set(SDecimal(100))
      }
      
      "campaign" >> {
        eval("count(load(//campaigns).campaign)") mustEqual Set(SDecimal(100))
      }
      
      "cpm" >> {
        eval("count(load(//campaigns).cpm)") mustEqual Set(SDecimal(100))
      }
      
      "ageRange" >> {
        eval("count(load(//campaigns).ageRange)") mustEqual Set(SDecimal(100))
      }
    }
    
    "evaluate the with operator across the campaigns dataset" in {
      val input = "count(load(//campaigns) with { t: 42 })"
      eval(input) mustEqual Set(SDecimal(100))
    }

    "map object creation over the campaigns dataset" in {
      val input = "{ aa: load(//campaigns).campaign }"
      val results = evalE(input)
      
      results must haveSize(100)
      
      forall(results) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveSize(1)
          obj must haveKey("aa")
        }
      }
    }
    
    "perform a naive cartesian product on the campaigns dataset" in {
      val input = """
        | a := load(//campaigns)
        | b := new a
        |
        | a ~ b
        |   { aa: a.campaign, bb: b.campaign }"""
        
      val results = evalE(input.stripMargin)
      
      results must haveSize(10000)
      
      forall(results) {
        case (VectorCase(_, _), SObject(obj)) => {
          obj must haveSize(2)
          obj must haveKey("aa")
          obj must haveKey("bb")
        }
      }
    }
    
    "determine a histogram of genders on campaigns" in {
      val input = """
        | campaigns := load(//campaigns)
        | hist('gender) :=
        |   { gender: 'gender, num: count(campaigns.gender where campaigns.gender = 'gender) }
        | hist""".stripMargin
        
      eval(input) mustEqual Set(
        SObject(Map("gender" -> SString("female"), "num" -> SDecimal(50))),
        SObject(Map("gender" -> SString("male"), "num" -> SDecimal(50))))
    }
    
    /* commented out until we have memoization (MASSIVE time sink)
    "determine a histogram of genders on category" in {
      val input = """
        | campaigns := load(//campaigns)
        | organizations := load(//organizations)
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
     
    "determine most isolated clicks in time" in {

      val input = """
        | clicks := load(//clicks)
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

      def time[T](f : => T): (T, Long) = {
        val start = System.currentTimeMillis
        val result = f
        (result, System.currentTimeMillis - start)
      }

      println("warmup")
      (1 to 10).foreach(_ => eval(input))

      println("warmup complete")
      Thread.sleep(30000)
      println("running")

      val runs = 25

      println("Avg run time = " + (time((1 to runs).map(_ => eval(input)))._2 / (runs * 1.0)) + "ms")
        
      //eval(input) mustEqual Set()   // TODO
      true mustEqual false
    }
    */
  
    "evaluate the 'hello, quirrel' examples" >> {
      "json" >> {
        "object" >> {
          val result = eval("""{ name: "John", age: 29, gender: "male" }""")
          result must haveSize(1)
          result must contain(SObject(Map("name" -> SString("John"), "age" -> SDecimal(29), "gender" -> SString("male"))))
        }
        
        "boolean" >> {
          val result = eval("true")
          result must haveSize(1)
          result must contain(SBoolean(true))
        }
        
        "string" >> {
          val result = eval("\"hello, world\"")
          result must haveSize(1)
          result must contain(SString("hello, world"))
        }
      }
      
      "numbers" >> {
        "addition" >> {
          val result = eval("5 + 2")
          result must haveSize(1)
          result must contain(SDecimal(7))
        }
        
        "multiplication" >> {
          val result = eval("8 * 2")
          result must haveSize(1)
          result must contain(SDecimal(16))
        }
      }
      
      "booleans" >> {
        "greater-than" >> {
          val result = eval("5 > 2")
          result must haveSize(1)
          result must contain(SBoolean(true))
        }
        
        "not-equal" >> {
          val result = eval("\"foo\" != \"foo\"")
          result must haveSize(1)
          result must contain(SBoolean(false))
        }
      }
      
      "variables" >> {
        "1" >> {
          val input = """
            | total := 2 + 1
            | total * 3""".stripMargin
            
          val result = eval(input)
          result must haveSize(1)
          result must contain(SDecimal(9))
        }
        
        "2" >> {
          val input = """
            | num := 4
            | square := num * num
            | square - 1""".stripMargin
            
          val result = eval(input)
          result must haveSize(1)
          result must contain(SDecimal(15))
        }
      }
    }
  }
  
  step {
    shutdown()
  }
  
  
  def eval(str: String): Set[SValue] = evalE(str) map { _._2 }
  
  def evalE(str: String) = {
    val tree = compile(str)
    tree.errors must beEmpty
    val Right(dag) = decorate(emit(tree))
    consumeEval("dummyUID", dag)
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
