package com.precog.yggdrasil
package table

import akka.dispatch.{Await, Future, ExecutionContext}
import akka.util.Duration

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._

import java.util.concurrent.Executors

import org.specs2._
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck

import scalaz.std.anyVal._

object GrouperSpec extends Specification with table.StubColumnarTableModule {
  import trans._
  
  implicit val asyncContext = ExecutionContext fromExecutor Executors.newCachedThreadPool()
  
  "grouping" should {
    "compute a histogram by value" in {
      val set = List(
        77,
        42,
        22,
        77,
        12,
        22,
        15,
        15,
        -71,
        22,
        42,
        77,
        22,
        0,
        15,
        53,
        -41,
        22,
        -41,
        0,
        22,
        77,
        69,
        42,
        0,
        53,
        22,
        22)
      
      val data = set map { JInt(_) }
        
      val spec = fromJson(data).group(TransSpec1.Id, 2,
        GroupKeySpecSource(JPathField("1"), TransSpec1.Id))
        
      val result = grouper.merge(spec) { (key: Table, map: Int => Table) =>
        val keyIter = key.toJson
        
        keyIter must haveSize(1)
        keyIter.head must beLike {
          case JInt(i) => set must contain(i)
        }
        
        val setIter = map(2).toJson
        
        setIter must not(beEmpty)
        forall(setIter) { i =>
          i mustEqual keyIter.head
        }
        
        Future(fromJson(JInt(setIter.size) :: Nil))
      }
      
      val resultIter = Await.result(result, Duration(10, "seconds")).toJson
      
      resultIter must haveSize(set.distinct.size)
      
      val expectedSet = (set.toSeq groupBy identity values) map { _.length } map { JInt(_) }
      
      forall(resultIter) { i => expectedSet must contain(i) }
    }
  }
}
