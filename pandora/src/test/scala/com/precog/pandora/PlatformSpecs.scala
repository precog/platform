package com.precog
package pandora

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
        | clicks := dataset(//clicks)
        | count(clicks where clicks.time > 0)""".stripMargin
        
      eval(input) mustEqual Set(SDecimal(100))
    }
    
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
        SObject(Map("gender" -> SString("female"), "num" -> SDecimal(50))),
        SObject(Map("gender" -> SString("male"), "num" -> SDecimal(50))))
    }
    
    /* commented out until we have memoization (MASSIVE time sink)
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
