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
import yggdrasil.actor._

import org.specs2.mutable._
  
import akka.dispatch.Await
import akka.util.Duration

import java.io.File

import scalaz._
import scalaz.effect.IO

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext

class PlatformSpecs extends Specification
    with ParseEvalStack
    with YggdrasilEnumOpsComponent
    with LevelDBQueryComponent 
    with DiskMemoizationComponent { platformSpecs =>

  lazy val controlTimeout = Duration(30, "seconds")      // it's just unreasonable to run tests longer than this
  
  lazy val actorSystem = ActorSystem("platform_specs_actor_system")
  implicit lazy val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

  trait YggConfig extends 
    BaseConfig with 
    YggEnumOpsConfig with 
    LevelDBQueryConfig with 
    DiskMemoizationConfig with 
    DatasetConsumersConfig with
    ProductionActorConfig

  object yggConfig extends YggConfig {
    lazy val config = Configuration parse {
      Option(System.getProperty("precog.storage.root")) map { "precog.storage.root = " + _ } getOrElse { "" }
    }

    lazy val flatMapTimeout = Duration(100, "seconds")
    lazy val projectionRetrievalTimeout = akka.util.Timeout(Duration(10, "seconds"))
    lazy val sortWorkDir = scratchDir
    lazy val chunkSerialization = BinaryProjectionSerialization
    lazy val memoizationBufferSize = sortBufferSize
    lazy val memoizationWorkDir = scratchDir
    lazy val maxEvalDuration = controlTimeout
  }

  lazy val Success(shardState) = YggState.restore(yggConfig.dataDir).unsafePerformIO

  type Storage = ActorYggShard
  val storage = new ActorYggShard with StandaloneActorEcosystem {
    type YggConfig = platformSpecs.YggConfig
    lazy val yggConfig = platformSpecs.yggConfig
    lazy val yggState = shardState 
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
        |   { aa: a.campaign, bb: b.campaign }""".stripMargin
        
      val results = evalE(input)
      
      results must haveSize(10000)
      
      forall(results) {
        case (VectorCase(_, _), SObject(obj)) => {
          obj must haveSize(2)
          obj must haveKey("aa")
          obj must haveKey("bb")
        }
      }
    }    

    "use where on a unioned set" in {
      val input = """
        | a := load(//campaigns) union load(//clicks)
        |   a where a.gender = "female" """.stripMargin
        
      val results = evalE(input)
      
      results must haveSize(46)
      
      forall(results) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveSize(5)
          obj must haveKey("gender")
        }
      }
    }
    
    //"return only value-unique results from a characteristic function" in {
    //  val input = """
    //    | campaigns := load(//campaigns)
    //    | f('a) :=
    //    |   campaigns.gender where campaigns.platform = 'a
    //    |
    //    | f""".stripMargin
    //    
    //  val results = evalE(input)
    //  
    //  results must haveSize(2)
    //  
    //  forall(results) {
    //    case (VectorCase(_), SString(gender)) =>
    //      Set("male", "female") must contain(gender)
    //  }
    //}
    
    "determine a histogram of genders on campaigns" in {
      val input = """
        | campaigns := load(//campaigns)
        | hist('gender) :=
        |   { gender: 'gender, num: count(campaigns.gender where campaigns.gender = 'gender) }
        | hist""".stripMargin
        
      eval(input) mustEqual Set(
        SObject(Map("gender" -> SString("female"), "num" -> SDecimal(46))),
        SObject(Map("gender" -> SString("male"), "num" -> SDecimal(54))))
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

      "outliers" >> {
        val input = """
           | campaigns := load(//campaigns)
           | bound := stdDev(campaigns.cpm)
           | avg := mean(campaigns.cpm)
           | outliers := campaigns where campaigns.cpm > (avg + bound)
           | outliers.platform""".stripMargin

          val result = eval(input)
          result must haveSize(5)
      }
      
      "should merge objects without timing out" >> {
        val input = """
           load(//richie1/test) 
        """.stripMargin

        eval(input) must not(throwA[Throwable])
      }.pendingUntilFixed

      // times out...
      /* "handle chained characteristic functions" in {
        val input = """
          | cust := load(//fs1/customers)
          | tran := load(//fs1/transactions)
          | relations('customer) :=
          |   cust' := cust where cust.customer = 'customer
          |   tran' := tran where tran.customer = 'customer
          |   tran' ~ cust'
          |     { country : cust'.country,  time : tran'.time, quantity : tran'.quantity }
          | grouping('country) :=
          |   { country: 'country, count: sum((relations where relations.country = 'country).quantity) }
          | grouping""".stripMargin

        val result = eval(input)
        result must haveSize(4)
      } */
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
    consumeEval("dummyUID", dag) match {
      case Success(result) => result
      case Failure(error) => throw error
    }
  }
  
  def startup() {
    // start storage shard 
    Await.result(storage.actorsStart, controlTimeout)
  }
  
  def shutdown() {
    // stop storage shard
    Await.result(storage.actorsStop, controlTimeout)
    
    actorSystem.shutdown()
  }
}
