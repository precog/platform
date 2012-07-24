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
package com.precog.yggdrasil
package table

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._

import java.util.concurrent.Executors

import org.specs2._
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck

import scalaz._
import scalaz.std.anyVal._
import scalaz.syntax.copointed._
import scalaz.syntax.monad._

trait GrouperSpec[M[+_]] extends Specification with StubColumnarTableModule[M] with ScalaCheck {
  import trans._

  implicit val coM: Copointed[M]
  
  "simple single-key grouping" should {
    "compute a histogram by value" in check { set: Stream[Int] =>
      val data = set map { JInt(_) }
        
      val spec = fromJson(data).group(TransSpec1.Id, 2,
        GroupKeySpecSource(JPathField("1"), TransSpec1.Id))
        
      val result = grouper.merge(spec) { (key: Table, map: Int => Table) =>
        for {
          keyIter <- key.toJson
          setIter <- map(2).toJson
        } yield {
          keyIter must haveSize(1)
          keyIter.head must beLike {
            case JInt(i) => set must contain(i)
          }
        
          setIter must not(beEmpty)
          forall(setIter) { i =>
            i mustEqual keyIter.head
          }
          
          fromJson(JInt(setIter.size) #:: Stream.empty)
        }
      }
      
      val resultIter = result.flatMap(_.toJson).copoint
      
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
        for {
          keyIter <- key.toJson
          setIter <- map(2).toJson
        } yield {
          keyIter must haveSize(1)
          keyIter.head must beLike {
            case JInt(i) => set must contain(i)
          }
          
          setIter must not(beEmpty)
          forall(setIter) {
            case JInt(v1) => {
              val JInt(v2) = keyIter.head
              v1 mustEqual (v2 * 2)
            }
          }
          
          fromJson(JInt(setIter.size) #:: Stream.empty)
        }
      }
      
      val resultIter = result.flatMap(_.toJson).copoint
      
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
        for {
          keyIter <- key.toJson
          setIter <- map(2).toJson
        } yield {
          keyIter must haveSize(1)
          keyIter.head must beLike {
            case JInt(i) => set must contain(i)
          }
          
          
          setIter must not(beEmpty)
          forall(setIter) {
            case JInt(v1) => {
              val JInt(v2) = keyIter.head
              (v1 % 2) mustEqual v2
            }
          }
          
          fromJson(JInt(setIter.size) #:: Stream.empty)
        }
      }
      
      val resultIter = result.flatMap(_.toJson).copoint
      
      resultIter must haveSize((set map { _ % 2 } distinct) size)
      
      val expectedSet = (set.toSeq groupBy { _ % 2 } values) map { _.length } map { JInt(_) }
      
      forall(resultIter) { i => expectedSet must contain(i) }
    }.pendingUntilFixed
  }
  
  "simple multi-key grouping" should {
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
    
    "compute a histogram on two keys" >> {
      "and" >> {
        val table = fromJson(data)
        
        val spec = table.group(
          TransSpec1.Id,
          3,
          GroupKeySpecAnd(
            GroupKeySpecSource(JPathField("1"), DerefObjectStatic(Leaf(Source), JPathField("a"))),
            GroupKeySpecSource(JPathField("2"), DerefObjectStatic(Leaf(Source), JPathField("b")))))
            
        val result = grouper.merge(spec) { (key, map) =>
          for {
            keyJson <- key.toJson
            gs1Json <- map(3).toJson
          } yield {
            keyJson must haveSize(1)
            
            keyJson.head must beLike {
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
            
            gs1Json must haveSize(1)
            fromJson(Stream(JInt(gs1Json.size)))
          }
        }
        
        val resultJson = result.flatMap(_.toJson).copoint
        
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
          for {
            keyJson <- key.toJson
            gs1Json <- map(3).toJson
          } yield {
            keyJson must haveSize(1)
            
            keyJson.head must beLike {
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
            
            fromJson(Stream(JInt(gs1Json.size)))
          }
        }
        
        val resultJson = result.flatMap(_.toJson).copoint
        resultJson must haveSize(10)
        
        forall(resultJson) { v =>
          v must beLike {
            case JInt(i) if i == 2 || i == 1 => ok
          }
        }
      }.pendingUntilFixed
    }
    
    "compute a histogram on one key with an extra" >> {
      val eq12F1 = new CF1P({
        case c: NumColumn => new Map1Column(c) with BoolColumn {
          def apply(row: Int) = c(row) == 12
        }
      })
      
      "and" >> {
        val table = fromJson(data)
        
        val spec = table.group(
          TransSpec1.Id,
          3,
          GroupKeySpecAnd(
            GroupKeySpecSource(JPathField("extra"),
              Filter(Map1(DerefObjectStatic(Leaf(Source), JPathField("a")), eq12F1), Map1(DerefObjectStatic(Leaf(Source), JPathField("a")), eq12F1))),
            GroupKeySpecSource(JPathField("2"), DerefObjectStatic(Leaf(Source), JPathField("b")))))
            
        val result = grouper.merge(spec) { (key, map) =>
          for {
            keyJson <- key.toJson
            gs1Json <- map(3).toJson
          } yield {
            keyJson must haveSize(1)
            
            keyJson must beLike {
              case obj: JObject => {
                val b = obj \ "2"
                
                b must beLike {
                  case JInt(i) if i == 7 => ok
                }
              }
            }
            
            gs1Json must haveSize(1)
            fromJson(Stream(JInt(gs1Json.size)))
          }
        }
        
        val resultJson = result.flatMap(_.toJson).copoint
        
        resultJson must haveSize(1)
        
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
            GroupKeySpecSource(JPathField("extra"),
              Filter(Map1(DerefObjectStatic(Leaf(Source), JPathField("a")), eq12F1), Map1(DerefObjectStatic(Leaf(Source), JPathField("a")), eq12F1))),
            GroupKeySpecSource(JPathField("2"), DerefObjectStatic(Leaf(Source), JPathField("b")))))
            
        val result = grouper.merge(spec) { (key, map) =>
          for {
            gs1Json <- map(3).toJson
            keyJson <- key.toJson
          } yield {
            gs1Json must haveSize(1)
            
            keyJson must haveSize(1)
            
            keyJson must beLike {
              case obj: JObject => {
                val b = obj \ "2"
                
                if (b == JNothing) {
                  gs1Json.head must beLike {
                    case subObj: JObject => {
                      (subObj \ "a") must beLike {
                        case JInt(i) if i == 12 => ok
                      }
                    }
                  }
                } else {
                  b must beLike {
                    case JInt(i) if i == 7 => gs1Json must haveSize(2)
                    case JInt(i) if i == 15 => gs1Json must haveSize(1)
                    case JInt(i) if i == -1 => gs1Json must haveSize(1)
                    case JInt(i) if i == 3 => gs1Json must haveSize(1)
                  }
                }
              }
            }
            
            fromJson(Stream(JInt(gs1Json.size)))
          }
        }
        
        val resultJson = result.flatMap(_.toJson).copoint
        
        resultJson must haveSize(5)
        
        forall(resultJson) { v =>
          v must beLike {
            case JInt(i) if i == 1 => ok
          }
        }
      }.pendingUntilFixed
    }
  }
  
  "multi-set grouping" should {
    "compute ctr on value" in check { (rawData1: Stream[Int], rawData2: Stream[Int]) =>
      val data1 = rawData1 map { JInt(_) }
      val data2 = rawData2 map { JInt(_) }
      
      val table1 = fromJson(data1)
      val table2 = fromJson(data2)
      
      val spec1 = table1.group(
        TransSpec1.Id,
        2,
        GroupKeySpecSource(JPathField("1"), TransSpec1.Id))
        
      val spec2 = table2.group(
        TransSpec1.Id,
        3,
        GroupKeySpecSource(JPathField("1"), TransSpec1.Id))
        
      val union = GroupingUnion(
        DerefObjectStatic(Leaf(Source), JPathField("1")),
        DerefObjectStatic(Leaf(Source), JPathField("1")),
        spec1,
        spec2,
        GroupKeyAlign.Eq)
          
      val result = grouper.merge(union) { (key, map) =>
        for {
          keyJson <- key.toJson
          gs1Json <- map(2).toJson
          gs2Json <- map(3).toJson
        } yield {
          keyJson must haveSize(1)
          
          keyJson.head must beLike {
            case obj: JObject => {
              val a = obj \ "1"
              
              a must beLike {
                case JInt(_) => ok
              }
            }
          }
          
          val JInt(keyBigInt) = keyJson.head \ "1"
          
          gs1Json must not(beEmpty)
          gs2Json must not(beEmpty)
          
          forall(gs1Json) { row =>
            row must beLike {
              case JInt(i) => i mustEqual keyBigInt
            }
          }
          
          forall(gs2Json) { row =>
            row must beLike {
              case JInt(i) => i mustEqual keyBigInt
            }
          }
          
          fromJson(Stream(
            JObject(
              JField("key", keyJson.head \ "1") ::
              JField("value", JInt(gs1Json.size + gs2Json.size)) :: Nil)))
        }
      }
      
      val resultJson = result.flatMap(_.toJson).copoint
      
      resultJson must haveSize((rawData1 ++ rawData2).distinct.length)
      
      forall(resultJson) { v =>
        v must beLike {
          case obj: JObject => {
            val JInt(k) = obj \ "key"
            val JInt(v) = obj \ "value"
            
            v mustEqual ((rawData1 ++ rawData2) filter { k == _ } length)
          }
        }
      }
    }.pendingUntilFixed
    
    "compute pair-sum join" in check { (rawData1: Stream[Int], rawData2: Stream[Int]) =>
      val data1 = rawData1 map { JInt(_) }
      val data2 = rawData2 map { JInt(_) }
      
      val table1 = fromJson(data1)
      val table2 = fromJson(data2)
      
      val spec1 = table1.group(
        TransSpec1.Id,
        2,
        GroupKeySpecSource(JPathField("1"), TransSpec1.Id))
        
      val spec2 = table2.group(
        TransSpec1.Id,
        3,
        GroupKeySpecSource(JPathField("1"), TransSpec1.Id))
        
      val union = GroupingIntersect(
        DerefObjectStatic(Leaf(Source), JPathField("1")),
        DerefObjectStatic(Leaf(Source), JPathField("1")),
        spec1,
        spec2,
        GroupKeyAlign.Eq)
          
      val result = grouper.merge(union) { (key, map) =>
        for {
          keyJson <- key.toJson
          gs1Json <- map(2).toJson
          gs2Json <- map(3).toJson
        } yield {
          keyJson must haveSize(1)
          
          keyJson.head must beLike {
            case obj: JObject => {
              val a = obj \ "1"
              
              a must beLike {
                case JInt(_) => ok
              }
            }
          }
          
          val JInt(keyBigInt) = keyJson.head \ "1"
          
          gs1Json must not(beEmpty)
          gs2Json must not(beEmpty)
          
          forall(gs1Json) { row =>
            row must beLike {
              case JInt(i) => i mustEqual keyBigInt
            }
          }
          
          forall(gs2Json) { row =>
            row must beLike {
              case JInt(i) => i mustEqual keyBigInt
            }
          }
          
          val JInt(v1) = gs1Json.head
          val JInt(v2) = gs2Json.head
          
          fromJson(Stream(
            JObject(
              JField("key", keyJson.head \ "1") ::
              JField("value", JInt(v1 + v2)) :: Nil)))
        }
      }
      
      val resultJson = result.flatMap(_.toJson).copoint
      
      resultJson must haveSize((rawData1 ++ rawData2).distinct.length)
      
      forall(resultJson) { v =>
        v must beLike {
          case obj: JObject => {
            val JInt(k) = obj \ "key"
            val JInt(v) = obj \ "value"
            
            v mustEqual (k * 2)
          }
        }
      }
    }.pendingUntilFixed
    
    "compute ctr on one field of a composite value" >> {
      "and" >> check { (rawData1: Stream[(Int, Option[Int])], rawData2: Stream[Int]) =>
        val data1 = rawData1 map {
          case (a, Some(b)) =>
            JObject(
              JField("a", JInt(a)) ::
              JField("b", JInt(b)) :: Nil)
              
          case (a, None) =>
            JObject(JField("a", JInt(a)) :: Nil)
        }
        
        val data2 = rawData2 map { a => JObject(JField("a", JInt(a)) :: Nil) }
        
        val table1 = fromJson(data1)
        val table2 = fromJson(data2)
        
        val spec1 = table1.group(
          TransSpec1.Id,
          2,
          GroupKeySpecAnd(
            GroupKeySpecSource(JPathField("1"),
              DerefObjectStatic(Leaf(Source), JPathField("a"))),
            GroupKeySpecSource(JPathField("2"),
              DerefObjectStatic(Leaf(Source), JPathField("b")))))
          
        val spec2 = table2.group(
          TransSpec1.Id,
          3,
          GroupKeySpecSource(JPathField("1"),
            DerefObjectStatic(Leaf(Source), JPathField("a"))))
          
        val union = GroupingUnion(
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          spec1,
          spec2,
          GroupKeyAlign.Eq)
            
        val result = grouper.merge(union) { (key, map) =>
          for {
            keyJson <- key.toJson
            gs1Json <- map(2).toJson
            gs2Json <- map(3).toJson
          } yield {
            keyJson must haveSize(1)
            
            keyJson.head must beLike {
              case obj: JObject => {
                val a = obj \ "1"
                val b = obj \ "2"
                
                a must beLike {
                  case JInt(_) => ok
                }
                
                b must beLike {
                  case JInt(_) => ok
                }
              }
            }
            
            val JInt(keyBigInt) = keyJson.head \ "1"
            
            gs1Json must not(beEmpty)
            gs2Json must not(beEmpty)
            
            forall(gs1Json) { row =>
              row must beLike {
                case obj: JObject => {
                  (obj \ "a") must beLike {
                    case JInt(i) => i mustEqual keyBigInt
                  }
                }
              }
            }
            
            forall(gs2Json) { row =>
              row must beLike {
                case obj: JObject => {
                  (obj \ "a") must beLike {
                    case JInt(i) => i mustEqual keyBigInt
                  }
                }
              }
            }
            
            fromJson(Stream(
              JObject(
                JField("key", keyJson.head \ "1") ::
                JField("value", JInt(gs1Json.size + gs2Json.size)) :: Nil)))
          }
        }
        
        val resultJson = result.flatMap(_.toJson).copoint
        
        resultJson must haveSize(((rawData1 map { _._1 }) ++ rawData2).distinct.length)
        
        forall(resultJson) { v =>
          v must beLike {
            case obj: JObject => {
              val JInt(k) = obj \ "key"
              val JInt(v) = obj \ "value"
              
              v mustEqual (((rawData1 map { _._1 }) ++ rawData2) filter { k == _ } length)
            }
          }
        }
      }.pendingUntilFixed
      
      "or" >> check { (rawData1: Stream[(Int, Option[Int])], rawData2: Stream[Int]) =>
        val data1 = rawData1 map {
          case (a, Some(b)) =>
            JObject(
              JField("a", JInt(a)) ::
              JField("b", JInt(b)) :: Nil)
              
          case (a, None) =>
            JObject(JField("a", JInt(a)) :: Nil)
        }
        
        val data2 = rawData2 map { a => JObject(JField("a", JInt(a)) :: Nil) }
        
        val table1 = fromJson(data1)
        val table2 = fromJson(data2)
        
        val spec1 = table1.group(
          TransSpec1.Id,
          2,
          GroupKeySpecOr(
            GroupKeySpecSource(JPathField("1"),
              DerefObjectStatic(Leaf(Source), JPathField("a"))),
            GroupKeySpecSource(JPathField("2"),
              DerefObjectStatic(Leaf(Source), JPathField("b")))))
          
        val spec2 = table2.group(
          TransSpec1.Id,
          3,
          GroupKeySpecSource(JPathField("1"),
            DerefObjectStatic(Leaf(Source), JPathField("a"))))
          
        val union = GroupingUnion(
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          spec1,
          spec2,
          GroupKeyAlign.Eq)
            
        val result = grouper.merge(union) { (key, map) =>
          for {
            keyJson <- key.toJson
            gs1Json <- map(2).toJson
            gs2Json <- map(3).toJson
          } yield {
            keyJson must haveSize(1)
            
            keyJson.head must beLike {
              case obj: JObject => {
                val a = obj \ "1"
                val b = obj \ "2"
                
                a must beLike {
                  case JInt(_) => ok
                }
                
                b must beLike {
                  case JInt(_) => ok
                  case JNothing => ok
                }
              }
            }
            
            val JInt(keyBigInt) = keyJson.head \ "1"
            
            gs1Json must not(beEmpty)
            gs2Json must not(beEmpty)
            
            forall(gs1Json) { row =>
              row must beLike {
                case obj: JObject => {
                  (obj \ "a") must beLike {
                    case JInt(i) => i mustEqual keyBigInt
                  }
                }
              }
            }
            
            forall(gs2Json) { row =>
              row must beLike {
                case obj: JObject => {
                  (obj \ "a") must beLike {
                    case JInt(i) => i mustEqual keyBigInt
                  }
                }
              }
            }
            
            fromJson(Stream(
              JObject(
                JField("key", keyJson.head \ "1") ::
                JField("value", JInt(gs1Json.size + gs2Json.size)) :: Nil)))
          }
        }
        
        val resultJson = result.flatMap(_.toJson).copoint
        
        resultJson must haveSize(((rawData1 map { _._1 }) ++ rawData2).distinct.length)
        
        forall(resultJson) { v =>
          v must beLike {
            case obj: JObject => {
              val JInt(k) = obj \ "key"
              val JInt(v) = obj \ "value"
              
              v mustEqual (((rawData1 map { _._1 }) ++ rawData2) filter { k == _ } length)
            }
          }
        }
      }.pendingUntilFixed
    }
    
    "compute pair-sum join on one field of a composite value" >> {
      "and" >> check { (rawData1: Stream[(Int, Option[Int])], rawData2: Stream[Int]) =>
        val data1 = rawData1 map {
          case (a, Some(b)) =>
            JObject(
              JField("a", JInt(a)) ::
              JField("b", JInt(b)) :: Nil)
              
          case (a, None) =>
            JObject(JField("a", JInt(a)) :: Nil)
        }
        
        val data2 = rawData2 map { a => JObject(JField("a", JInt(a)) :: Nil) }
        
        val table1 = fromJson(data1)
        val table2 = fromJson(data2)
        
        val spec1 = table1.group(
          TransSpec1.Id,
          2,
          GroupKeySpecAnd(
            GroupKeySpecSource(JPathField("1"),
              DerefObjectStatic(Leaf(Source), JPathField("a"))),
            GroupKeySpecSource(JPathField("2"),
              DerefObjectStatic(Leaf(Source), JPathField("b")))))
          
        val spec2 = table2.group(
          TransSpec1.Id,
          3,
          GroupKeySpecSource(JPathField("1"),
            DerefObjectStatic(Leaf(Source), JPathField("a"))))
          
        val union = GroupingIntersect(
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          spec1,
          spec2,
          GroupKeyAlign.Eq)
            
        val result = grouper.merge(union) { (key, map) =>
          for {
            keyJson <- key.toJson
            gs1Json <- map(2).toJson
            gs2Json <- map(3).toJson
          } yield {
            keyJson must haveSize(1)
            
            keyJson.head must beLike {
              case obj: JObject => {
                val a = obj \ "1"
                val b = obj \ "2"
                
                a must beLike {
                  case JInt(_) => ok
                }
                
                b must beLike {
                  case JInt(_) => ok
                }
              }
            }
            
            val JInt(keyBigInt) = keyJson.head \ "1"
            
            gs1Json must not(beEmpty)
            gs2Json must not(beEmpty)
            
            forall(gs1Json) { row =>
              row must beLike {
                case obj: JObject => {
                  (obj \ "a") must beLike {
                    case JInt(i) => i mustEqual keyBigInt
                  }
                }
              }
            }
            
            forall(gs2Json) { row =>
              row must beLike {
                case obj: JObject => {
                  (obj \ "a") must beLike {
                    case JInt(i) => i mustEqual keyBigInt
                  }
                }
              }
            }
          
            val JInt(v1) = gs1Json.head \ "a"
            val JInt(v2) = gs2Json.head \ "a"
            
            fromJson(Stream(
              JObject(
                JField("key", keyJson.head \ "1") ::
                JField("value", JInt(v1 + v2)) :: Nil)))
          }
        }
        
        val resultJson = result.flatMap(_.toJson).copoint
        
        resultJson must haveSize(((rawData1 map { _._1 }) ++ rawData2).distinct.length)
        
        forall(resultJson) { v =>
          v must beLike {
            case obj: JObject => {
              val JInt(k) = obj \ "key"
              val JInt(v) = obj \ "value"
              
              v mustEqual (k * 2)
            }
          }
        }
      }.pendingUntilFixed
      
      "or" >> check { (rawData1: Stream[(Int, Option[Int])], rawData2: Stream[Int]) =>
        val data1 = rawData1 map {
          case (a, Some(b)) =>
            JObject(
              JField("a", JInt(a)) ::
              JField("b", JInt(b)) :: Nil)
              
          case (a, None) =>
            JObject(JField("a", JInt(a)) :: Nil)
        }
        
        val data2 = rawData2 map { a => JObject(JField("a", JInt(a)) :: Nil) }
        
        val table1 = fromJson(data1)
        val table2 = fromJson(data2)
        
        val spec1 = table1.group(
          TransSpec1.Id,
          2,
          GroupKeySpecOr(
            GroupKeySpecSource(JPathField("1"),
              DerefObjectStatic(Leaf(Source), JPathField("a"))),
            GroupKeySpecSource(JPathField("2"),
              DerefObjectStatic(Leaf(Source), JPathField("b")))))
          
        val spec2 = table2.group(
          TransSpec1.Id,
          3,
          GroupKeySpecSource(JPathField("1"),
            DerefObjectStatic(Leaf(Source), JPathField("a"))))
          
        val union = GroupingUnion(
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          spec1,
          spec2,
          GroupKeyAlign.Eq)
            
        val result = grouper.merge(union) { (key, map) =>
          for {
            keyJson <- key.toJson
            gs1Json <- map(2).toJson
            gs2Json <- map(3).toJson
          } yield {
            
            keyJson must haveSize(1)
            
            keyJson.head must beLike {
              case obj: JObject => {
                val a = obj \ "1"
                val b = obj \ "2"
                
                a must beLike {
                  case JInt(_) => ok
                }
                
                b must beLike {
                  case JInt(_) => ok
                  case JNothing => ok
                }
              }
            }
            
            val JInt(keyBigInt) = keyJson.head \ "1"
            
            gs1Json must not(beEmpty)
            gs2Json must not(beEmpty)
            
            forall(gs1Json) { row =>
              row must beLike {
                case obj: JObject => {
                  (obj \ "a") must beLike {
                    case JInt(i) => i mustEqual keyBigInt
                  }
                }
              }
            }
            
            forall(gs2Json) { row =>
              row must beLike {
                case obj: JObject => {
                  (obj \ "a") must beLike {
                    case JInt(i) => i mustEqual keyBigInt
                  }
                }
              }
            }
          
            val JInt(v1) = gs1Json.head \ "a"
            val JInt(v2) = gs2Json.head \ "a"
            
            fromJson(Stream(
              JObject(
                JField("key", keyJson.head \ "1") ::
                JField("value", JInt(v1 + v2)) :: Nil)))
          }
        }
        
        val resultJson = result.flatMap(_.toJson).copoint
        
        resultJson must haveSize(((rawData1 map { _._1 }) ++ rawData2).distinct.length)
        
        forall(resultJson) { v =>
          v must beLike {
            case obj: JObject => {
              val JInt(k) = obj \ "key"
              val JInt(v) = obj \ "value"
              
              v mustEqual (k * 2)
            }
          }
        }
      }.pendingUntilFixed
    }
  }
}
