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

import com.precog.yggdrasil.util.IdSourceConfig
import com.precog.yggdrasil.test._

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._

import java.util.concurrent.Executors

import org.specs2.ScalaCheck
import org.specs2.mutable._

import scalaz._
import scalaz.std.anyVal._
import scalaz.syntax.copointed._
import scalaz.syntax.monad._

trait GrouperSpec[M[+_]] extends BlockStoreTestSupport[M] with Specification with ScalaCheck {
  def tic_a = JPathField("tic_a")
  def tic_b = JPathField("tic_b")

  def testHistogramByValue(set: Stream[Int]) = {
    val module = emptyTestModule
    import module._
    import trans._
    import constants._
    
    val data = set.zipWithIndex map { case (v, i) => JObject(JField("key", JArray(JNum(i) :: Nil)) :: JField("value", JNum(v)) :: Nil) }

    val groupId = module.newGroupId
      
    val spec = GroupingSource(
      fromJson(data), 
      SourceKey.Single, Some(TransSpec1.Id), groupId, 
      GroupKeySpecSource(tic_a, SourceValue.Single))
      
    val result = Table.merge(spec) { (key: Table, map: GroupId => M[Table]) =>
      for {
        keyIter <- key.toJson
        group2  <- map(groupId)
        setIter <- group2.toJson
      } yield {
        keyIter must haveSize(1)
        keyIter.head must beLike {
          case JObject(JField("tic_a", JNum(i)) :: Nil) => set must contain(i)
        }

        val histoKey = keyIter.head(tic_a)
        val JNum(histoKey0) = histoKey
        val histoKeyInt = histoKey0.toInt
      
        setIter must not(beEmpty)
        forall(setIter) { record =>
          (record \ "value") mustEqual histoKey
        }

        setIter.size must_== set.count(_ == histoKeyInt)
        
        fromJson(JNum(setIter.size) #:: Stream.empty)
      }
    }
    
    val resultIter = result.flatMap(_.toJson).copoint
    
    resultIter must haveSize(set.distinct.size)
    
    val expectedSet = (set.toSeq groupBy identity values) map { _.length } map { JNum(_) }
    
    forall(resultIter) { i => expectedSet must contain(i) }
  }
  
  def testHistogramByValueMapped(set: Stream[Int]) = {
    val module = emptyTestModule
    import module._
    import trans._
    import constants._

    val data = set.zipWithIndex map { case (v, i) => JObject(JField("key", JArray(JNum(i) :: Nil)) :: JField("value", JNum(v)) :: Nil) }
    
    val doubleF1 = new CF1P({
      case c: LongColumn => new Map1Column(c) with LongColumn {
        def apply(row: Int) = c(row) * 2
      }
    })

    val groupId = module.newGroupId

    val valueTrans = InnerObjectConcat(
      WrapObject(SourceKey.Single, TableModule.paths.Key.name),
      WrapObject(Map1(SourceValue.Single, doubleF1), TableModule.paths.Value.name))
    
    val spec = GroupingSource(
      fromJson(data), 
      SourceKey.Single, Some(valueTrans), groupId, 
      GroupKeySpecSource(tic_a, SourceValue.Single))
      
    val result = Table.merge(spec) { (key: Table, map: GroupId => M[Table]) =>
      for {
        keyIter <- key.toJson
        group2  <- map(groupId)
        setIter <- group2.toJson
      } yield {
        keyIter must haveSize(1)
        keyIter.head must beLike {
          case JObject(JField("tic_a", JNum(i)) :: Nil) => set must contain(i)
        }
        
        val histoKey = keyIter.head(tic_a)
        val JNum(histoKey0) = histoKey
        val histoKeyInt = histoKey0.toInt
      
        setIter must not(beEmpty)
        forall(setIter) { record =>
          val JNum(v2) = (record \ "value") 
          v2 must_== (histoKey0 * 2)
        }
        
        fromJson(JNum(setIter.size) #:: Stream.empty)
      }
    }
    
    val resultIter = result.flatMap(_.toJson).copoint
    
    resultIter must haveSize(set.distinct.size)
    
    val expectedSet = (set.toSeq groupBy identity values) map { _.length } map { JNum(_) }
    
    forall(resultIter) { i => expectedSet must contain(i) }
  }

  def testHistogramEvenOdd(set: Stream[Int]) = {
    val module = emptyTestModule
    import module._
    import trans._
    import constants._

    val data = set.zipWithIndex map { case (v, i) => JObject(JField("key", JArray(JNum(i) :: Nil)) :: JField("value", JNum(v)) :: Nil) }
    
    val mod2 = new CF1P({
      case c: LongColumn => new Map1Column(c) with LongColumn {
        def apply(row: Int) = c(row) % 2
      }
    })

    val groupId = module.newGroupId
    
    val spec = GroupingSource(
      fromJson(data),
      SourceKey.Single, Some(TransSpec1.Id), groupId, 
      GroupKeySpecSource(tic_a, Map1(SourceValue.Single, mod2)))
      
    val result = Table.merge(spec) { (key: Table, map: GroupId => M[Table]) =>
      for {
        keyIter <- key.toJson
        group2  <- map(groupId)
        setIter <- group2.toJson
      } yield {
        keyIter must haveSize(1)
        keyIter.head must beLike {
          case JObject(JField("tic_a", JNum(i)) :: Nil) => set.map(_ % 2) must contain(i)
        }

        val histoKey = keyIter.head(tic_a)
        val JNum(histoKey0) = histoKey
        val histoKeyInt = histoKey0.toInt
        
        setIter must not(beEmpty)
        forall(setIter) { record =>
          val JNum(v0) = (record \ "value") 
          histoKeyInt must_== (v0.toInt % 2)
        }
        
        fromJson(JNum(setIter.size) #:: Stream.empty)
      }
    }
    
    val resultIter = result.flatMap(_.toJson).copoint
    
    resultIter must haveSize((set map { _ % 2 } distinct) size)
    
    val expectedSet = (set.toSeq groupBy { _ % 2 } values) map { _.length } map { JNum(_) }
    
    forall(resultIter) { i => expectedSet must contain(i) }
  }

  def simpleMultiKeyData = {
    val JArray(elements) = JsonParser.parse("""[
      { "key": [0], "value": {"a": 12, "b": 7} },
      { "key": [1], "value": {"a": 42} },
      { "key": [2], "value": {"a": 11, "c": true} },
      { "key": [3], "value": {"a": 12} },
      { "key": [4], "value": {"b": 15} },
      { "key": [5], "value": {"b": -1, "c": false} },
      { "key": [6], "value": {"b": 7} },
      { "key": [7], "value": {"a": -7, "b": 3, "d": "testing"} },
    ]""")

    elements.toStream
  }

  def testHistogramTwoKeysAnd = {
    val module = emptyTestModule
    import module._
    import trans._
    import constants._

    val table = fromJson(simpleMultiKeyData)

    val groupId = newGroupId
    
    val spec = GroupingSource(
      table,
      SourceKey.Single, Some(TransSpec1.Id), groupId,
      GroupKeySpecAnd(
        GroupKeySpecSource(tic_a, DerefObjectStatic(SourceValue.Single, JPathField("a"))),
        GroupKeySpecSource(tic_b, DerefObjectStatic(SourceValue.Single, JPathField("b")))))
        
    val result = Table.merge(spec) { (key, map) =>
      for {
        keyJson <- key.toJson
        group3  <- map(groupId)
        gs1Json <- group3.toJson
      } yield {
        keyJson must haveSize(1)
        keyJson.head must beLike {
          case obj: JObject => {
            val a = obj(tic_a)
            val b = obj(tic_b)
            
            a must beLike {
              case JNum(i) if i == 12 => {
                b must beLike {
                  case JNum(i) if i == 7 => ok
                }
              }
              
              case JNum(i) if i == -7 => {
                b must beLike {
                  case JNum(i) if i == 3 => ok
                }
              }
            }
          }
        }
        
        gs1Json must haveSize(1)
        fromJson(Stream(JNum(gs1Json.size)))
      }
    }
    
    val resultJson = result.flatMap(_.toJson).copoint
    
    resultJson must haveSize(2)
    
    forall(resultJson) { v =>
      v must beLike {
        case JNum(i) if i == 1 => ok
      }
    }
  }

  def testHistogramTwoKeysOr = {
    val module = emptyTestModule
    import module._
    import trans._
    import constants._

    val table = fromJson(simpleMultiKeyData)
    
    val groupId = newGroupId
    
    val spec = GroupingSource(
      table,
      SourceKey.Single, Some(TransSpec1.Id), groupId,
      GroupKeySpecOr(
        GroupKeySpecSource(tic_a, DerefObjectStatic(SourceValue.Single, JPathField("a"))),
        GroupKeySpecSource(tic_b, DerefObjectStatic(SourceValue.Single, JPathField("b")))))
        
    val result = Table.merge(spec) { (key, map) =>
      for {
        keyJson <- key.toJson
        group3  <- map(groupId)
        gs1Json <- group3.toJson
      } yield {
        keyJson must haveSize(1)
        keyJson.head must beLike {
          case obj: JObject => {
            val a = obj(tic_a)
            val b = obj(tic_b)
        
            if (a == JNothing) {
              b must beLike {
                case JNum(i) if i == 7 => gs1Json must haveSize(2)
                case JNum(i) if i == 15 => gs1Json must haveSize(1)
                case JNum(i) if i == -1 => gs1Json must haveSize(1)
                case JNum(i) if i == 3 => gs1Json must haveSize(1)
              }
            } else if (b == JNothing) {
              a must beLike {
                case JNum(i) if i == 12 => gs1Json must haveSize(2)
                case JNum(i) if i == 42 => gs1Json must haveSize(1)
                case JNum(i) if i == 11 => gs1Json must haveSize(1)
                case JNum(i) if i == -7 => gs1Json must haveSize(1)
              }
            } else {
              a must beLike {
                case JNum(i) if i == 12 => {
                  b must beLike {
                    case JNum(i) if i == 7 => ok
                  }
                }
                  
                case JNum(i) if i == -7 => {
                  b must beLike {
                    case JNum(i) if i == 3 => ok
                  }
                }
              }
              
              gs1Json must haveSize(1)
            }
          }
        }
        
        fromJson(Stream(JNum(gs1Json.size)))
      }
    }
    
    val resultJson = result.flatMap(_.toJson).copoint
    resultJson must haveSize(8)
    
    forall(resultJson) { v =>
      v must beLike {
        case JNum(i) if i == 2 || i == 1 => ok
      }
    }
  }

  val eq12F1 = new CF1P({
    case c: DoubleColumn => new Map1Column(c) with BoolColumn {
      def apply(row: Int) = c(row) == 12.0d
    }
    case c: LongColumn => new Map1Column(c) with BoolColumn {
      def apply(row: Int) = c(row) == 12l
    }
    case c: NumColumn => new Map1Column(c) with BoolColumn {
      def apply(row: Int) = c(row.toInt) == 12
    }
  })
  
  def testHistogramExtraAnd = {
    val module = emptyTestModule
    import module._
    import trans._
    import constants._

    val table = fromJson(simpleMultiKeyData)
    val groupId = newGroupId
    
    val spec = GroupingSource(
      table,
      SourceKey.Single, Some(TransSpec1.Id), groupId,
      GroupKeySpecAnd(
        GroupKeySpecSource(JPathField("extra"),
          Filter(Map1(DerefObjectStatic(SourceValue.Single, JPathField("a")), eq12F1), Map1(DerefObjectStatic(SourceValue.Single, JPathField("a")), eq12F1))),
        GroupKeySpecSource(tic_b, DerefObjectStatic(SourceValue.Single, JPathField("b")))))
        
    val result = Table.merge(spec) { (key, map) =>
      for {
        keyJson <- key.toJson
        group3  <- map(groupId)
        gs1Json <- group3.toJson
      } yield {
        keyJson must haveSize(1)
        
        (keyJson.head(tic_b)) must beLike {
          case JNum(i) => i must_== 7
        }
        
        gs1Json must haveSize(1)
        fromJson(Stream(JNum(gs1Json.size)))
      }
    }
    
    val resultJson = result.flatMap(_.toJson).copoint
    
    resultJson must haveSize(1)
    
    forall(resultJson) { v =>
      v must beLike {
        case JNum(i) if i == 1 => ok
      }
    }
  }

  def testHistogramExtraOr = {
    val module = emptyTestModule
    import module._
    import trans._
    import constants._

    val table = fromJson(simpleMultiKeyData)
    val groupId = newGroupId
    
    // data where data.b = 'b | data.a = 12
    val spec = GroupingSource(
      table,
      SourceKey.Single, Some(SourceValue.Single), groupId,
      GroupKeySpecOr(
        GroupKeySpecSource(JPathField("extra"),
          Filter(Map1(DerefObjectStatic(SourceValue.Single, JPathField("a")), eq12F1), Map1(DerefObjectStatic(SourceValue.Single, JPathField("a")), eq12F1))),
        GroupKeySpecSource(tic_b, DerefObjectStatic(SourceValue.Single, JPathField("b")))))
        
    val result = Table.merge(spec) { (key, map) =>
      for {
        group3  <- map(groupId)
        gs1Json <- group3.toJson
        keyJson <- key.toJson
      } yield {
        keyJson must haveSize(1)

        (keyJson.head(tic_b)) must beLike {
          case JNothing =>
            (gs1Json.head \ "a") must beLike {
              case JNum(i) if i == 12 => ok
            }

          case JNum(i) if i == 7 => gs1Json must haveSize(2)
          case JNum(i) if i == 15 => gs1Json must haveSize(1)
          case JNum(i) if i == -1 => gs1Json must haveSize(1)
          case JNum(i) if i == 3 => gs1Json must haveSize(1)
        }
        

        fromJson(Stream(JArray(keyJson.head :: JNum(gs1Json.size) :: Nil)))
      }
    }
    
    val resultJson = result.flatMap(_.toJson).copoint
    
    resultJson must haveSize(5)
    
    val JArray(expected) = JsonParser.parse("""[[{"tic_b": 7}, 2], [{"tic_b": 15}, 1], [{"tic_b": -1}, 1], [{"tic_b": 3}, 1], [{"extra": true}, 2]]""")
    resultJson.toSet must_== expected.toSet
  }

  def testCtr(rawData1: Stream[Int], rawData2: Stream[Int]) = {
    val module = emptyTestModule
    import module._
    import trans._
    import constants._

    val data1 = rawData1.zipWithIndex map { case (v, i) => JObject(JField("key", JArray(JNum(i) :: Nil)) :: JField("value", JNum(v)) :: Nil) }
    val data2 = rawData2.zipWithIndex map { case (v, i) => JObject(JField("key", JArray(JNum(i) :: Nil)) :: JField("value", JNum(v)) :: Nil) }
    
    val table1 = fromJson(data1)
    val table2 = fromJson(data2)

    val groupId1 = newGroupId
    val groupId2 = newGroupId
    
    // t1 where t1 = 'a
    val spec1 = GroupingSource(
      table1,
      SourceKey.Single, Some(SourceValue.Single), groupId1,
      GroupKeySpecSource(tic_a, SourceValue.Single))
      
    // t2 where t2 = 'a
    val spec2 = GroupingSource(
      table2,
      SourceKey.Single, Some(SourceValue.Single), groupId2,
      GroupKeySpecSource(tic_a, SourceValue.Single))
      
    val intersect = GroupingAlignment(
      DerefObjectStatic(Leaf(Source), tic_a),
      DerefObjectStatic(Leaf(Source), tic_a),
      spec1,
      spec2, GroupingSpec.Intersection)
        
    val result = Table.merge(intersect) { (key, map) =>
      for {
        keyJson <- key.toJson
        group2  <- map(groupId1)
        group3  <- map(groupId2)
        gs1Json <- group2.toJson
        gs2Json <- group3.toJson
      } yield {
        keyJson must haveSize(1)
        
        val JNum(keyBigInt) = keyJson.head(tic_a)

        forall(gs1Json) { row =>
          row must beLike {
            case JNum(i) => i mustEqual keyBigInt
          }
        }
        
        gs1Json must haveSize(rawData1.count(_ == keyBigInt.toInt))
        gs2Json must haveSize(rawData2.count(_ == keyBigInt.toInt))

        fromJson(Stream(
          JObject(
            JField("key", keyJson.head(tic_a)) ::
            JField("value", JNum(gs1Json.size + gs2Json.size)) :: Nil)))
      }
    }
    
    val resultJson = result.flatMap(_.toJson).copoint
    
    resultJson must haveSize((rawData1.toSet intersect rawData2.toSet).size)
    
    forall(resultJson) { record =>
      val JNum(k) = record \ "key"
      val JNum(v) = record \ "value"
      
      v mustEqual ((rawData1 ++ rawData2) filter { k == _ } length)
    }
  }

  def testCtrPartialJoin(rawData1: Stream[(Int, Option[Int])], rawData2: Stream[Int]) = { 
    val module = emptyTestModule
    import module._
    import trans._
    import constants._

    val data1 = rawData1.zipWithIndex map {
      case ((a, b0), i) =>
        JObject(
          JField("key", JArray(JNum(i) :: Nil)) :: 
          JField("value", JObject(JField("a", JNum(a)) :: b0.map(b => JField("b", JNum(b))).toList)) :: Nil)
    }
    
    val data2 = rawData2.zipWithIndex map { 
      case (v, i) => 
        JObject(
          JField("key", JArray(JNum(i) :: Nil)) :: 
          JField("value", JObject(JField("a", JNum(v)) :: Nil)) :: Nil) 
    }
    
    val table1 = fromJson(data1)
    val table2 = fromJson(data2)
    
    val groupId1 = newGroupId
    val groupId2 = newGroupId
    
    val spec1 = GroupingSource(
      table1,
      SourceKey.Single, Some(SourceValue.Single), groupId1,
      GroupKeySpecAnd(
        GroupKeySpecSource(tic_a, DerefObjectStatic(SourceValue.Single, JPathField("a"))),
        GroupKeySpecSource(tic_b, DerefObjectStatic(SourceValue.Single, JPathField("b")))))
      
    val spec2 = GroupingSource(
      table2,
      SourceKey.Single, Some(SourceValue.Single), groupId2,
      GroupKeySpecSource(tic_a, DerefObjectStatic(SourceValue.Single, JPathField("a"))))
      
    val intersection = GroupingAlignment(
      DerefObjectStatic(Leaf(Source), tic_a),
      DerefObjectStatic(Leaf(Source), tic_a),
      spec1,
      spec2, GroupingSpec.Intersection)
        
    val result = Table.merge(intersection) { (key, map) =>
      for {
        keyJson <- key.toJson
        group2  <- map(groupId1)
        group3  <- map(groupId2)
        gs1Json <- group2.toJson
        gs2Json <- group3.toJson
      } yield {
        keyJson must haveSize(1)

        val JNum(keyBigInt) = keyJson.head(tic_a)
        keyJson.head(tic_b) must beLike {
          case JNum(_) => ok
        }
        
        gs1Json must not(beEmpty)
        gs2Json must not(beEmpty)
        
        //println("Got intersection with key " + keyJson.toList)
        //println("left: \n" + gs1Json.toList)
        //println("right: \n"+ gs2Json.toList)
        
        forall(gs1Json) { row =>
          (row \ "a") must beLike {
            case JNum(i) => i mustEqual keyBigInt
          }
        }
        
        forall(gs2Json) { row =>
          (row \ "a") must beLike {
            case JNum(i) => i mustEqual keyBigInt
          }
        }
        
        fromJson(Stream(
          JObject(
            JField("key", JArray(keyJson.head(tic_a) :: keyJson.head(tic_b) :: Nil)) ::
            JField("value", JNum(gs1Json.size + gs2Json.size)) :: Nil)))
      }
    }
    
    val resultJson = result.flatMap(_.toJson).copoint
    
    val joinKeys = (rawData1.map(_._1).toSet intersect rawData2.toSet)

    val grouped1 = rawData1.filter({ case (a, b) => joinKeys(a) }).groupBy(_._1)
    val grouped2 = rawData2.groupBy(identity[Int]).filterKeys(joinKeys)

    // in order to get a binding for both 'a and 'b, values must come from
    // rawData1 and 'a must be in the join keys.
    resultJson must haveSize(rawData1.filter({ case (a, b) => joinKeys(a) }).distinct.size)

    val grouped1ab = grouped1.mapValues(_.groupBy(_._2.get))
    forall(resultJson) { v =>
      v must beLike {
        case obj: JObject => {
          val JArray(JNum(ka) :: JNum(kb) :: Nil) = obj \ "key"
          val JNum(v) = obj \ "value"
          
          v must_== (grouped1ab(ka.toInt)(kb.toInt).size + grouped2(ka.toInt).size)
        }
      }
    }
  }

  "simple single-key grouping" should {
    "scalacheck a histogram by value" in check (testHistogramByValue _)
    "histogram for two of the same value" in testHistogramByValue(Stream(2147483647, 2147483647))
    "histogram when observing spans of equal values" in testHistogramByValue(Stream(24, -10, 0, -1, -1, 0, 24, 0, 0, 24, -1, 0, 0, 24))
    "compute a histogram by value (mapping target)" in check (testHistogramByValueMapped _)
    "compute a histogram by value (mapping target) trivial example" in testHistogramByValueMapped(Stream(0))
    "compute a histogram by even/odd" in check (testHistogramEvenOdd _)
    "compute a histogram by even/odd trivial example" in testHistogramEvenOdd(Stream(0))
  }

  "simple multi-key grouping" should {
    "compute a histogram on two keys" >> {
      "and" >> testHistogramTwoKeysAnd
      "or" >> testHistogramTwoKeysOr
    }
    "compute a histogram on one key with an extra" >> {
      "and" >> testHistogramExtraAnd
      "or" >> testHistogramExtraOr
    }
  }

"multi-set grouping" should {
    "compute ctr on value" in propNoShrink (testCtr _)
    "compute ctr with an empty dataset" in testCtr(Stream(), Stream(1))
    "compute ctr with singleton datasets" in testCtr(Stream(1), Stream(1))
    "compute ctr with simple datasets with repeats" in testCtr(Stream(1, 1, 1), Stream(1))
    "compute ctr with simple datasets" in testCtr(Stream(-565998477, 1911906594, 1), Stream(1948335811, -528723320, 1))
    "compute ctr with simple datasets" in testCtr(Stream(1, 2147483647, 2126441435, -1, 0, 0), Stream(2006322377, -2147483648, -1456034303, 2147483647, 0, 2147483647, -1904025337))

    "compute ctr on one field of a composite value" >> {
      "and" >> propNoShrink (testCtrPartialJoin _)
      "and with un-joinable datasets" >> testCtrPartialJoin(
        Stream((0,Some(1)), (1123021019,Some(-2147483648))), 
        Stream(-1675865668, 889796884, 2147483647, -1099860336, -2147483648, -2147483648, 1, 1496400141)
      )
      "and with joinable datasets" >> testCtrPartialJoin(
        Stream((-1,Some(-1771882715)), (-2091150211,Some(1)), (1,Some(-1161386492)), (0,Some(-1)), (-1,Some(-1)), (-2147483648,Some(-2147483648)), (-1,Some(1)), (0,Some(391541906)), (-2147483648,Some(725820706)), (0,Some(-2147483648)), (1286585203,Some(560695941))), 
        Stream(0, -297579588, -1, 2147483647, -1, -1536865491, 1049246142, -2147483648, -2147483648, 766980226, -1047565460)
      )
      "and with repeated group keys in joinable datasets" >> testCtrPartialJoin(
        Stream((1,Some(-421523375)), (1381663801,Some(2145939312)), (975603510,Some(-456843566)), (-260964705,Some(-811947401)), (-1643830562,Some(0)), (382901678,Some(-2147483648)), (-1770905652,Some(-1)), (1172197808,Some(1)), (-206421051,Some(307500840)), (2147483647,Some(-1)), (2147483647,Some(-1)), (-1775980054,Some(2147483647))), 
        Stream(1, -1, -2005746103, 720318134, 852618110, 1813748094, -1, -1676020815, -627348537, 2147483647, -2147483648)
      )
    }
  }
}

/*
      "or" >> check { (rawData1: Stream[(Int, Option[Int])], rawData2: Stream[Int]) =>
        val module = emptyTestModule
        import module._
        import trans._
        import constants._

        val data1 = rawData1 map {
          case (a, Some(b)) =>
            JObject(
              JField("a", JNum(a)) ::
              JField("b", JNum(b)) :: Nil)
              
          case (a, None) =>
            JObject(JField("a", JNum(a)) :: Nil)
        }
        
        val data2 = rawData2 map { a => JObject(JField("a", JNum(a)) :: Nil) }
        
        val table1 = fromJson(data1)
        val table2 = fromJson(data2)
        
        val spec1 = GroupingSource(
          table1,
          SourceKey.Single, Some(TransSpec1.Id), 2,
          GroupKeySpecOr(
            GroupKeySpecSource(JPathField("1"),
              DerefObjectStatic(Leaf(Source), JPathField("a"))),
            GroupKeySpecSource(JPathField("2"),
              DerefObjectStatic(Leaf(Source), JPathField("b")))))
          
        val spec2 = GroupingSource(
          table2,
          SourceKey.Single, Some(TransSpec1.Id), 3,
          GroupKeySpecSource(JPathField("1"),
            DerefObjectStatic(Leaf(Source), JPathField("a"))))
          
        val union = GroupingAlignment(
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          spec1,
          spec2, GroupingSpec.Union)
            
        val result = Table.merge(union) { (key, map) =>
          for {
            keyJson <- key.toJson
            group2  <- map(2)
            group3  <- map(3)
            gs1Json <- group2.toJson
            gs2Json <- group3.toJson
          } yield {
            keyJson must haveSize(1)
            
            keyJson.head must beLike {
              case obj: JObject => {
                val a = obj \ "1"
                val b = obj \ "2"
                
                a must beLike {
                  case JNum(_) => ok
                }
                
                b must beLike {
                  case JNum(_) => ok
                  case JNothing => ok
                }
              }
            }
            
            val JNum(keyBigInt) = keyJson.head \ "1"
            
            gs1Json must not(beEmpty)
            gs2Json must not(beEmpty)
            
            forall(gs1Json) { row =>
              row must beLike {
                case obj: JObject => {
                  (obj \ "a") must beLike {
                    case JNum(i) => i mustEqual keyBigInt
                  }
                }
              }
            }
            
            forall(gs2Json) { row =>
              row must beLike {
                case obj: JObject => {
                  (obj \ "a") must beLike {
                    case JNum(i) => i mustEqual keyBigInt
                  }
                }
              }
            }
            
            fromJson(Stream(
              JObject(
                JField("key", keyJson.head \ "1") ::
                JField("value", JNum(gs1Json.size + gs2Json.size)) :: Nil)))
          }
        }
        
        val resultJson = result.flatMap(_.toJson).copoint
        
        resultJson must haveSize(((rawData1 map { _._1 }) ++ rawData2).distinct.length)
        
        forall(resultJson) { v =>
          v must beLike {
            case obj: JObject => {
              val JNum(k) = obj \ "key"
              val JNum(v) = obj \ "value"
              
              v mustEqual (((rawData1 map { _._1 }) ++ rawData2) filter { k == _ } length)
            }
          }
        }
      }.pendingUntilFixed
    }
    
    //
    // forall 'a forall 'b
    //   foo where foo.a = 'a & foo.b = 'b
    //   bar where bar.a = 'a
    //   baz where baz.b = 'b
    //   
    //   -- note: intersect, not union!  (inexpressible in Quirrel)
    //   { a: 'a, b: 'b, foo: count(foo'), bar: count(bar'), baz: count(baz') }
    //
     
    "handle non-trivial group alignment with composite key" in {
      val module = emptyTestModule
      import module._
      import trans._
      import constants._

      val foo = Stream(
        JObject(
          JField("a", JNum(42)) ::    // 1
          JField("b", JNum(12)) :: Nil),
        JObject(
          JField("a", JNum(42)) :: Nil),
        JObject(
          JField("a", JNum(77)) :: Nil),
        JObject(
          JField("c", JNum(-3)) :: Nil),
        JObject(
          JField("b", JNum(7)) :: Nil),
        JObject(
          JField("a", JNum(42)) ::    // 1
          JField("b", JNum(12)) :: Nil),
        JObject(
          JField("a", JNum(7)) ::     // 2
          JField("b", JNum(42)) :: Nil),
        JObject(
          JField("a", JNum(17)) ::    // 3
          JField("b", JNum(6)) :: Nil),
        JObject(
          JField("b", JNum(1)) :: Nil),
        JObject(
          JField("a", JNum(21)) ::    // 4
          JField("b", JNum(12)) :: Nil),
        JObject(
          JField("a", JNum(42)) ::    // 5
          JField("b", JNum(-2)) :: Nil),
        JObject(
          JField("c", JNum(-3)) :: Nil),
        JObject(
          JField("a", JNum(7)) ::     // 2
          JField("b", JNum(42)) :: Nil),
        JObject(
          JField("a", JNum(42)) ::    // 1
          JField("b", JNum(12)) :: Nil))
          
      val bar = Stream(
        JObject(
          JField("a", JNum(42)) :: Nil),    // 1
        JObject(
          JField("a", JNum(42)) :: Nil),    // 1
        JObject(
          JField("a", JNum(77)) :: Nil),    // 6
        JObject(
          JField("c", JNum(-3)) :: Nil),
        JObject(
          JField("b", JNum(7)) :: Nil),
        JObject(
          JField("b", JNum(12)) :: Nil),
        JObject(
          JField("a", JNum(7)) ::           // 2
          JField("b", JNum(42)) :: Nil),
        JObject(
          JField("a", JNum(17)) ::          // 3
          JField("c", JNum(77)) :: Nil),
        JObject(
          JField("b", JNum(1)) :: Nil),
        JObject(
          JField("b", JNum(12)) :: Nil),
        JObject(
          JField("b", JNum(-2)) :: Nil),
        JObject(
          JField("c", JNum(-3)) :: Nil),
        JObject(
          JField("a", JNum(7)) :: Nil),     // 2
        JObject(
          JField("a", JNum(42)) :: Nil))    // 1
          
      val baz = Stream(
        JObject(
          JField("b", JNum(12)) :: Nil),    // 1
        JObject(
          JField("b", JNum(6)) :: Nil),     // 3
        JObject(
          JField("a", JNum(42)) :: Nil),
        JObject(
          JField("b", JNum(1)) :: Nil),     // 7
        JObject(
          JField("b", JNum(12)) :: Nil),    // 1
        JObject(
          JField("c", JNum(-3)) :: Nil),
        JObject(
          JField("b", JNum(42)) :: Nil),    // 2
        JObject(
          JField("d", JNum(0)) :: Nil))
          
      val fooSpec = GroupingSource(
        fromJson(foo),
        SourceKey.Single, Some(TransSpec1.Id), 3,
        GroupKeySpecAnd(
          GroupKeySpecSource(
            JPathField("1"),
            DerefObjectStatic(Leaf(Source), JPathField("a"))),
          GroupKeySpecSource(
            JPathField("2"),
            DerefObjectStatic(Leaf(Source), JPathField("b")))))
          
      val barSpec = GroupingSource(
        fromJson(bar),
        SourceKey.Single, Some(TransSpec1.Id), 4,
        GroupKeySpecSource(
          JPathField("1"),
          DerefObjectStatic(Leaf(Source), JPathField("a"))))
          
      val bazSpec = GroupingSource(
        fromJson(baz),
        SourceKey.Single, Some(TransSpec1.Id), 5,
        GroupKeySpecSource(
          JPathField("2"),
          DerefObjectStatic(Leaf(Source), JPathField("b"))))
          
      "intersect" >> {
        val spec = GroupingAlignment(
          DerefObjectStatic(Leaf(Source), JPathField("2")),
          DerefObjectStatic(Leaf(Source), JPathField("2")),
          GroupingAlignment(
            DerefObjectStatic(Leaf(Source), JPathField("1")),
            DerefObjectStatic(Leaf(Source), JPathField("1")),
            fooSpec,
            barSpec, GroupingSpec.Intersection),
          bazSpec, GroupingSpec.Intersection)
          
        val forallResult = Table.merge(spec) { (key, map) =>
          val keyJson = key.toJson.copoint
          
          keyJson must not(beEmpty)
          
          val a = keyJson.head \ "1"
          val b = keyJson.head \ "2"
          
          a mustNotEqual JNothing
          b mustNotEqual JNothing
          
          for {
            group3 <- map(3)
            group4 <- map(4)
            group5 <- map(5)
            fooPJson <- group3.toJson
            barPJson <- group4.toJson
            bazPJson <- group5.toJson
          } yield {
            fooPJson must not(beEmpty)
            barPJson must not(beEmpty)
            bazPJson must not(beEmpty)

            val result = Stream(
              JObject(
                JField("a", a) ::
                JField("b", b) ::
                JField("foo", JNum(fooPJson.size)) ::
                JField("bar", JNum(barPJson.size)) ::
                JField("baz", JNum(bazPJson.size)) :: Nil))
                
            fromJson(result)
          }
        }
        
        val forallJson = forallResult flatMap { _.toJson } copoint
        
        forallJson must not(beEmpty)
        forallJson must haveSize(2)
        
        forall(forallJson) { row =>
          row must beLike {
            case obj: JObject if (obj \ "a") == JNum(42) => {
              val JNum(ai) = obj \ "a"
              val JNum(bi) = obj \ "b"
              
              ai mustEqual 42
              bi mustEqual 12
              
              val JNum(fooi) = obj \ "foo"
              val JNum(bari) = obj \ "bar"
              val JNum(bazi) = obj \ "baz"
              
              fooi mustEqual 3
              bari mustEqual 3
              bazi mustEqual 2
            }
            
            case obj: JObject if (obj \ "a") == JNum(7) => {
              val JNum(ai) = obj \ "a"
              val JNum(bi) = obj \ "b"
              
              ai mustEqual 7
              bi mustEqual 42
              
              val JNum(fooi) = obj \ "foo"
              val JNum(bari) = obj \ "bar"
              val JNum(bazi) = obj \ "baz"
              
              fooi mustEqual 2
              bari mustEqual 2
              bazi mustEqual 1
            }
          }
        }
      }.pendingUntilFixed
    }
  }
}
*/

//object GrouperSpec extends TableModuleSpec[Free.Trampoline] with GrouperSpec[Free.Trampoline] {
//  implicit def M = Trampoline.trampolineMonad
//
//  type YggConfig = IdSourceConfig
//  val yggConfig = new IdSourceConfig {
//    val idSource = new IdSource {
//      private val source = new java.util.concurrent.atomic.AtomicLong
//      def nextId() = source.getAndIncrement
//    }
//  }
//}
//
//object GrouperSpec extends TableModuleSpec[akka.dispatch.Future] with GrouperSpec[akka.dispatch.Future] {
//  import akka.actor.ActorSystem
//  import akka.dispatch._
//  import akka.util.Duration
//  import akka.util.duration._
//
//  val actorSystem = ActorSystem("grouperSpecActorSystem")
//  implicit val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
//  implicit val M: Monad[Future] with Copointed[Future] = new blueeyes.bkka.FutureMonad(asyncContext) with Copointed[Future] {
//    def copoint[A](f: Future[A]) = Await.result(f, Duration(30, "seconds"))
//  }
//
//  type YggConfig = IdSourceConfig
//  val yggConfig = new IdSourceConfig {
//    val idSource = new IdSource {
//      private val source = new java.util.concurrent.atomic.AtomicLong
//      def nextId() = source.getAndIncrement
//    }
//  }
//}
//

object GrouperSpec extends TableModuleSpec[YId] with GrouperSpec[YId] with YIdInstances {
  type YggConfig = IdSourceConfig
  val yggConfig = new IdSourceConfig {
    val idSource = new IdSource {
      private val source = new java.util.concurrent.atomic.AtomicLong
      def nextId() = source.getAndIncrement
    }
  }
}



