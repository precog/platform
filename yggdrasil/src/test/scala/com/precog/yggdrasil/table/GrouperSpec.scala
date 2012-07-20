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

object GrouperSpec extends Specification with table.StubColumnarTableModule with ScalaCheck {
  import trans._
  
  implicit val asyncContext = ExecutionContext fromExecutor Executors.newCachedThreadPool()
  
  "simple single-key grouping" should {
    "compute a histogram by value" in check { set: Stream[Int] =>
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
        
        Future(fromJson(JInt(setIter.size) #:: Stream.empty))
      }
      
      val resultIter = Await.result(result, Duration(10, "seconds")).toJson
      
      resultIter must haveSize(set.distinct.size)
      
      val expectedSet = (set.toSeq groupBy identity values) map { _.length } map { JInt(_) }
      
      forall(resultIter) { i => expectedSet must contain(i) }
    }.pendingUntilFixed
    
    "compute a histogram by value (mapping target)" in check { set: Stream[Int] =>
      val data = set map { JInt(_) }
      
      val doubleF1 = new CF1P({
        case c: NumColumn => new Map1Column(c) with NumColumn {
          def apply(row: Int) = c(row) * 2
        }
      })
      
      val spec = fromJson(data).group(Map1(Leaf(Source), doubleF1), 2,
        GroupKeySpecSource(JPathField("1"), TransSpec1.Id))
        
      val result = grouper.merge(spec) { (key: Table, map: Int => Table) =>
        val keyIter = key.toJson
        
        keyIter must haveSize(1)
        keyIter.head must beLike {
          case JInt(i) => set must contain(i)
        }
        
        val setIter = map(2).toJson
        
        setIter must not(beEmpty)
        forall(setIter) {
          case JInt(v1) => {
            val JInt(v2) = keyIter.head
            v1 mustEqual (v2 * 2)
          }
        }
        
        Future(fromJson(JInt(setIter.size) #:: Stream.empty))
      }
      
      val resultIter = Await.result(result, Duration(10, "seconds")).toJson
      
      resultIter must haveSize(set.distinct.size)
      
      val expectedSet = (set.toSeq groupBy identity values) map { _.length } map { JInt(_) }
      
      forall(resultIter) { i => expectedSet must contain(i) }
    }.pendingUntilFixed
    
    "compute a histogram by even/odd" in check { set: Stream[Int] =>
      val data = set map { JInt(_) }
      
      val mod2 = new CF1P({
        case c: NumColumn => new Map1Column(c) with NumColumn {
          def apply(row: Int) = c(row) % 2
        }
      })
      
      val spec = fromJson(data).group(TransSpec1.Id, 2,
        GroupKeySpecSource(JPathField("1"), Map1(Leaf(Source), mod2)))
        
      val result = grouper.merge(spec) { (key: Table, map: Int => Table) =>
        val keyIter = key.toJson
        
        keyIter must haveSize(1)
        keyIter.head must beLike {
          case JInt(i) => set must contain(i)
        }
        
        val setIter = map(2).toJson
        
        setIter must not(beEmpty)
        forall(setIter) {
          case JInt(v1) => {
            val JInt(v2) = keyIter.head
            (v1 % 2) mustEqual v2
          }
        }
        
        Future(fromJson(JInt(setIter.size) #:: Stream.empty))
      }
      
      val resultIter = Await.result(result, Duration(10, "seconds")).toJson
      
      resultIter must haveSize((set map { _ % 2 } distinct) size)
      
      val expectedSet = (set.toSeq groupBy { _ % 2 } values) map { _.length } map { JInt(_) }
      
      forall(resultIter) { i => expectedSet must contain(i) }
    }.pendingUntilFixed
  }
  
  "simple multi-key grouping" should {
    "compute a histogram on two keys" >> {
      val data = Stream(
        JObject(
          JField("a", JInt(12)) ::
          JField("b", JInt(7)) :: Nil),
        JObject(
          JField("a", JInt(42)) :: Nil),
        JObject(
          JField("a", JInt(11)) ::
          JField("c", JBool(true)) :: Nil),
        JObject(
          JField("a", JInt(12)) :: Nil),
        JObject(
          JField("b", JInt(15)) :: Nil),
        JObject(
          JField("b", JInt(-1)) ::
          JField("c", JBool(false)) :: Nil),
        JObject(
          JField("b", JInt(7)) :: Nil),
        JObject(
          JField("a", JInt(-7)) ::
          JField("b", JInt(3)) ::
          JField("d", JString("testing")) :: Nil))
      
      "and" >> {
        val table = fromJson(data)
        
        val spec = table.group(
          TransSpec1.Id,
          3,
          GroupKeySpecAnd(
            GroupKeySpecSource(JPathField("1"), DerefObjectStatic(Leaf(Source), JPathField("a"))),
            GroupKeySpecSource(JPathField("2"), DerefObjectStatic(Leaf(Source), JPathField("b")))))
            
        val result = grouper.merge(spec) { (key, map) =>
          val keyJson = key.toJson.toSeq
          keyJson must haveSize(1)
          
          keyJson must beLike {
            case obj: JObject => {
              val a = obj \ "1"
              val b = obj \ "2"
              
              a must beLike {
                case JInt(i) if i == 12 => {
                  b must beLike {
                    case JInt(i) if i == 7 => ok
                  }
                }
                
                case JInt(i) if i == -7 => {
                  b must beLike {
                    case JInt(i) if i == 3 => ok
                  }
                }
              }
            }
          }
          
          val gs1Json = map(3).toJson.toSeq
          
          gs1Json must haveSize(1)
          Future(fromJson(Stream(JInt(gs1Json.size))))
        }
        
        val resultJson = Await.result(result, Duration(10, "seconds")).toJson.toSeq
        
        resultJson must haveSize(2)
        
        forall(resultJson) { v =>
          v must beLike {
            case JInt(i) if i == 1 => ok
          }
        }
      }.pendingUntilFixed
      
      "or" >> {
        val table = fromJson(data)
        
        val spec = table.group(
          TransSpec1.Id,
          3,
          GroupKeySpecOr(
            GroupKeySpecSource(JPathField("1"), DerefObjectStatic(Leaf(Source), JPathField("a"))),
            GroupKeySpecSource(JPathField("2"), DerefObjectStatic(Leaf(Source), JPathField("b")))))
            
        val result = grouper.merge(spec) { (key, map) =>
          val keyJson = key.toJson.toSeq
          keyJson must haveSize(1)
          
          val gs1Json = map(3).toJson.toSeq
          
          keyJson must beLike {
            case obj: JObject => {
              val a = obj \ "1"
              val b = obj \ "2"
          
              if (a == JNothing) {
                b must beLike {
                  case JInt(i) if i == 7 => gs1Json must haveSize(2)
                  case JInt(i) if i == 15 => gs1Json must haveSize(1)
                  case JInt(i) if i == -1 => gs1Json must haveSize(1)
                  case JInt(i) if i == 3 => gs1Json must haveSize(1)
                }
              } else if (b == JNothing) {
                a must beLike {
                  case JInt(i) if i == 12 => gs1Json must haveSize(2)
                  case JInt(i) if i == 42 => gs1Json must haveSize(1)
                  case JInt(i) if i == 11 => gs1Json must haveSize(1)
                  case JInt(i) if i == -7 => gs1Json must haveSize(1)
                }
              } else {
                a must beLike {
                  case JInt(i) if i == 12 => {
                    b must beLike {
                      case JInt(i) if i == 7 => ok
                    }
                  }
                    
                  case JInt(i) if i == -7 => {
                    b must beLike {
                      case JInt(i) if i == 3 => ok
                    }
                  }
                }
                
                gs1Json must haveSize(1)
              }
            }
          }
          
          Future(fromJson(Stream(JInt(gs1Json.size))))
        }
        
        val resultJson = Await.result(result, Duration(10, "seconds")).toJson.toSeq
        resultJson must haveSize(10)
        
        forall(resultJson) { v =>
          v must beLike {
            case JInt(i) if i == 2 => ok
            case JInt(i) if i == 1 => ok
          }
        }
      }.pendingUntilFixed
    }
  }
}
