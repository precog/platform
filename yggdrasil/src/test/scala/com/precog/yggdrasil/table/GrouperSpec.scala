package com.precog.yggdrasil
package table

import com.precog.yggdrasil.util.IdSourceConfig

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
  "simple single-key grouping" should {
    "compute a histogram by value" in check { set: Stream[Int] =>
      //println("===========================================================")
      val module = emptyTestModule
      import module._
      import trans._
      import constants._
      
      val data = set map { JNum(_) }

      val groupId = module.newGroupId
        
      val spec = GroupingSource(
        fromJson(data), 
        TransSpec1.Id, Some(TransSpec1.Id), groupId, 
        GroupKeySpecSource(JPathField("tic_a"), TransSpec1.Id))
        
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

          val histoKey = keyIter.head \ "tic_a"
          val JNum(histoKey0) = histoKey
          val histoKeyInt = histoKey0.toInt
        
          setIter must not(beEmpty)
          //println(setIter.mkString("\n"))
          forall(setIter) { i =>
            i mustEqual histoKey
          }

          setIter.size must_== set.count(_ == histoKeyInt)
          
          fromJson(JNum(setIter.size) #:: Stream.empty)
        }
      }
      
      val resultIter = result.flatMap(_.toJson).copoint
      
      resultIter must haveSize(set.distinct.size)
      
      val expectedSet = (set.toSeq groupBy identity values) map { _.length } map { JNum(_) }
      
      forall(resultIter) { i => expectedSet must contain(i) }
    }.pendingUntilFixed
  }
}
    /*
    
    "compute a histogram by value (mapping target)" in check { set: Stream[Int] =>
      val module = emptyTestModule
      import module._
      import trans._
      import constants._

      val data = set map { JNum(_) }
      
      val doubleF1 = new CF1P({
        case c: NumColumn => new Map1Column(c) with NumColumn {
          def apply(row: Int) = c(row) * 2
        }
      })
      
      val spec = GroupingSource(
        fromJson(data), 
        SourceKey.Single, Some(Map1(TransSpec1.Id, doubleF1)), 2, 
        GroupKeySpecSource(JPathField("1"), TransSpec1.Id))
        
      val result = Table.merge(spec) { (key: Table, map: GroupId => M[Table]) =>
        for {
          keyIter <- key.toJson
          group2  <- map(2)
          setIter <- group2.toJson
        } yield {
          keyIter must haveSize(1)
          keyIter.head must beLike {
            case JNum(i) => set must contain(i)
            case _ => sys.error("Expected a JNum")
          }
          
          setIter must not(beEmpty)
          forall(setIter) {
            case JNum(v1) => {
              val JNum(v2) = keyIter.head
              v1 mustEqual (v2 * 2)
            }
            case _ => sys.error("Expected a JNum")
          }
          
          fromJson(JNum(setIter.size) #:: Stream.empty)
        }
      }
      
      val resultIter = result.flatMap(_.toJson).copoint
      
      resultIter must haveSize(set.distinct.size)
      
      val expectedSet = (set.toSeq groupBy identity values) map { _.length } map { JNum(_) }
      
      forall(resultIter) { i => expectedSet must contain(i) }
    }.pendingUntilFixed
    
    "compute a histogram by even/odd" in check { set: Stream[Int] =>
      val module = emptyTestModule
      import module._
      import trans._
      import constants._

      val data = set map { JNum(_) }
      
      val mod2 = new CF1P({
        case c: NumColumn => new Map1Column(c) with NumColumn {
          def apply(row: Int) = c(row) % 2
        }
      })
      
      val spec = GroupingSource(
        fromJson(data),
        SourceKey.Single, Some(TransSpec1.Id), 2, 
        GroupKeySpecSource(JPathField("1"), Map1(Leaf(Source), mod2)))
        
      val result = Table.merge(spec) { (key: Table, map: Int => M[Table]) =>
        for {
          keyIter <- key.toJson
          group2  <- map(2)
          setIter <- group2.toJson
        } yield {
          keyIter must haveSize(1)
          keyIter.head must beLike {
            case JNum(i) => set must contain(i)
            case _ => sys.error("Expected a JNum")
          }
          
          
          setIter must not(beEmpty)
          forall(setIter) {
            case JNum(v1) => {
              val JNum(v2) = keyIter.head
              (v1 % 2) mustEqual v2
            }
            case _ => sys.error("Expected a JNum")
          }
          
          fromJson(JNum(setIter.size) #:: Stream.empty)
        }
      }
      
      val resultIter = result.flatMap(_.toJson).copoint
      
      resultIter must haveSize((set map { _ % 2 } distinct) size)
      
      val expectedSet = (set.toSeq groupBy { _ % 2 } values) map { _.length } map { JNum(_) }
      
      forall(resultIter) { i => expectedSet must contain(i) }
    }.pendingUntilFixed
  }
  
  "simple multi-key grouping" should {
    val module = emptyTestModule
    import module._
    import trans._
    import constants._

    val data = Stream(
      JObject(
        JField("a", JNum(12)) ::
        JField("b", JNum(7)) :: Nil),
      JObject(
        JField("a", JNum(42)) :: Nil),
      JObject(
        JField("a", JNum(11)) ::
        JField("c", JBool(true)) :: Nil),
      JObject(
        JField("a", JNum(12)) :: Nil),
      JObject(
        JField("b", JNum(15)) :: Nil),
      JObject(
        JField("b", JNum(-1)) ::
        JField("c", JBool(false)) :: Nil),
      JObject(
        JField("b", JNum(7)) :: Nil),
      JObject(
        JField("a", JNum(-7)) ::
        JField("b", JNum(3)) ::
        JField("d", JString("testing")) :: Nil))
    
    "compute a histogram on two keys" >> {
      "and" >> {
        val module = emptyTestModule
        import module._
        import trans._
        import constants._

        val table = fromJson(data)
        
        val spec = GroupingSource(
          table,
          SourceKey.Single, Some(SourceValue.Single), 3,
          GroupKeySpecAnd(
            GroupKeySpecSource(JPathField("1"), DerefObjectStatic(Leaf(Source), JPathField("a"))),
            GroupKeySpecSource(JPathField("2"), DerefObjectStatic(Leaf(Source), JPathField("b")))))
            
        val result = Table.merge(spec) { (key, map) =>
          for {
            keyJson <- key.toJson
            group3  <- map(3)
            gs1Json <- group3.toJson
          } yield {
            keyJson must haveSize(1)
            
            keyJson.head must beLike {
              case obj: JObject => {
                val a = obj \ "1"
                val b = obj \ "2"
                
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
      }.pendingUntilFixed
      
      "or" >> {
        val module = emptyTestModule
        import module._
        import trans._
        import constants._

        val table = fromJson(data)
        
        val spec = GroupingSource(
          table,
          SourceKey.Single, Some(TransSpec1.Id), 3,
          GroupKeySpecOr(
            GroupKeySpecSource(JPathField("1"), DerefObjectStatic(Leaf(Source), JPathField("a"))),
            GroupKeySpecSource(JPathField("2"), DerefObjectStatic(Leaf(Source), JPathField("b")))))
            
        val result = Table.merge(spec) { (key, map) =>
          for {
            keyJson <- key.toJson
            group3  <- map(3)
            gs1Json <- group3.toJson
          } yield {
            keyJson must haveSize(1)
            
            keyJson.head must beLike {
              case obj: JObject => {
                val a = obj \ "1"
                val b = obj \ "2"
            
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
        resultJson must haveSize(10)
        
        forall(resultJson) { v =>
          v must beLike {
            case JNum(i) if i == 2 || i == 1 => ok
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
        val module = emptyTestModule
        import module._
        import trans._
        import constants._

        val table = fromJson(data)
        
        val spec = GroupingSource(
          table,
          SourceKey.Single, Some(TransSpec1.Id), 3,
          GroupKeySpecAnd(
            GroupKeySpecSource(JPathField("extra"),
              Filter(Map1(DerefObjectStatic(Leaf(Source), JPathField("a")), eq12F1), Map1(DerefObjectStatic(Leaf(Source), JPathField("a")), eq12F1))),
            GroupKeySpecSource(JPathField("2"), DerefObjectStatic(Leaf(Source), JPathField("b")))))
            
        val result = Table.merge(spec) { (key, map) =>
          for {
            keyJson <- key.toJson
            group3  <- map(3)
            gs1Json <- group3.toJson
          } yield {
            keyJson must haveSize(1)
            
            keyJson must beLike {
              case obj: JObject => {
                val b = obj \ "2"
                
                b must beLike {
                  case JNum(i) if i == 7 => ok
                }
              }
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
      }.pendingUntilFixed
      
      "or" >> {
        val module = emptyTestModule
        import module._
        import trans._
        import constants._

        val table = fromJson(data)
        
        val spec = GroupingSource(
          table,
          SourceKey.Single, Some(SourceValue.Single), 3,
          GroupKeySpecOr(
            GroupKeySpecSource(JPathField("extra"),
              Filter(Map1(DerefObjectStatic(Leaf(Source), JPathField("a")), eq12F1), Map1(DerefObjectStatic(Leaf(Source), JPathField("a")), eq12F1))),
            GroupKeySpecSource(JPathField("2"), DerefObjectStatic(Leaf(Source), JPathField("b")))))
            
        val result = Table.merge(spec) { (key, map) =>
          for {
            group3  <- map(3)
            gs1Json <- group3.toJson
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
                        case JNum(i) if i == 12 => ok
                      }
                    }
                  }
                } else {
                  b must beLike {
                    case JNum(i) if i == 7 => gs1Json must haveSize(2)
                    case JNum(i) if i == 15 => gs1Json must haveSize(1)
                    case JNum(i) if i == -1 => gs1Json must haveSize(1)
                    case JNum(i) if i == 3 => gs1Json must haveSize(1)
                  }
                }
              }
            }
            
            fromJson(Stream(JNum(gs1Json.size)))
          }
        }
        
        val resultJson = result.flatMap(_.toJson).copoint
        
        resultJson must haveSize(5)
        
        forall(resultJson) { v =>
          v must beLike {
            case JNum(i) if i == 1 => ok
          }
        }
      }.pendingUntilFixed
    }
  }
  
  "multi-set grouping" should {
    "compute ctr on value" in check { (rawData1: Stream[Int], rawData2: Stream[Int]) =>
      val module = emptyTestModule
      import module._
      import trans._
      import constants._

      val data1 = rawData1 map { JNum(_) }
      val data2 = rawData2 map { JNum(_) }
      
      val table1 = fromJson(data1)
      val table2 = fromJson(data2)
      
      val spec1 = GroupingSource(
        table1,
        SourceKey.Single, Some(TransSpec1.Id), 2,
        GroupKeySpecSource(JPathField("1"), TransSpec1.Id))
        
      val spec2 = GroupingSource(
        table2,
        SourceKey.Single, Some(TransSpec1.Id), 3,
        GroupKeySpecSource(JPathField("1"), TransSpec1.Id))
        
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
              
              a must beLike {
                case JNum(_) => ok
              }
            }
          }
          
          val JNum(keyBigInt) = keyJson.head \ "1"
          
          gs1Json must not(beEmpty)
          gs2Json must not(beEmpty)
          
          forall(gs1Json) { row =>
            row must beLike {
              case JNum(i) => i mustEqual keyBigInt
            }
          }
          
          forall(gs2Json) { row =>
            row must beLike {
              case JNum(i) => i mustEqual keyBigInt
            }
          }
          
          fromJson(Stream(
            JObject(
              JField("key", keyJson.head \ "1") ::
              JField("value", JNum(gs1Json.size + gs2Json.size)) :: Nil)))
        }
      }
      
      val resultJson = result.flatMap(_.toJson).copoint
      
      resultJson must haveSize((rawData1 ++ rawData2).distinct.length)
      
      forall(resultJson) { v =>
        v must beLike {
          case obj: JObject => {
            val JNum(k) = obj \ "key"
            val JNum(v) = obj \ "value"
            
            v mustEqual ((rawData1 ++ rawData2) filter { k == _ } length)
          }
        }
      }
    }.pendingUntilFixed
    
    "compute pair-sum join" in check { (rawData1: Stream[Int], rawData2: Stream[Int]) =>
      val module = emptyTestModule
      import module._
      import trans._
      import constants._

      val data1 = rawData1 map { JNum(_) }
      val data2 = rawData2 map { JNum(_) }
      
      val table1 = fromJson(data1)
      val table2 = fromJson(data2)
      
      val spec1 = GroupingSource(
        table1,
        SourceKey.Single, Some(TransSpec1.Id), 2,
        GroupKeySpecSource(JPathField("1"), TransSpec1.Id))
        
      val spec2 = GroupingSource(
        table2,
        SourceKey.Single, Some(TransSpec1.Id), 3,
        GroupKeySpecSource(JPathField("1"), TransSpec1.Id))
        
      val intersect = GroupingAlignment(
        DerefObjectStatic(Leaf(Source), JPathField("1")),
        DerefObjectStatic(Leaf(Source), JPathField("1")),
        spec1,
        spec2, GroupingSpec.Intersection)
          
      val result = Table.merge(intersect) { (key, map) =>
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
              
              a must beLike {
                case JNum(_) => ok
              }
            }
          }
          
          val JNum(keyBigInt) = keyJson.head \ "1"
          
          gs1Json must not(beEmpty)
          gs2Json must not(beEmpty)
          
          forall(gs1Json) { row =>
            row must beLike {
              case JNum(i) => i mustEqual keyBigInt
            }
          }
          
          forall(gs2Json) { row =>
            row must beLike {
              case JNum(i) => i mustEqual keyBigInt
            }
          }
          
          val JNum(v1) = gs1Json.head
          val JNum(v2) = gs2Json.head
          
          fromJson(Stream(
            JObject(
              JField("key", keyJson.head \ "1") ::
              JField("value", JNum(v1 + v2)) :: Nil)))
        }
      }
      
      val resultJson = result.flatMap(_.toJson).copoint
      
      resultJson must haveSize((rawData1 ++ rawData2).distinct.length)
      
      forall(resultJson) { v =>
        v must beLike {
          case obj: JObject => {
            val JNum(k) = obj \ "key"
            val JNum(v) = obj \ "value"
            
            v mustEqual (k * 2)
          }
        }
      }
    }.pendingUntilFixed
    
    "compute ctr on one field of a composite value" >> {
      "and" >> check { (rawData1: Stream[(Int, Option[Int])], rawData2: Stream[Int]) =>
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
          GroupKeySpecAnd(
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
    
    "compute pair-sum join on one field of a composite value" >> {
      "and" >> check { (rawData1: Stream[(Int, Option[Int])], rawData2: Stream[Int]) =>
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
          GroupKeySpecAnd(
            GroupKeySpecSource(JPathField("1"),
              DerefObjectStatic(Leaf(Source), JPathField("a"))),
            GroupKeySpecSource(JPathField("2"),
              DerefObjectStatic(Leaf(Source), JPathField("b")))))
          
        val spec2 = GroupingSource(
          table2,
          SourceKey.Single, Some(TransSpec1.Id), 3,
          GroupKeySpecSource(JPathField("1"),
            DerefObjectStatic(Leaf(Source), JPathField("a"))))
          
        val intersect = GroupingAlignment(
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          spec1,
          spec2, GroupingSpec.Intersection)
            
        val result = Table.merge(intersect) { (key, map) =>
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
          
            val JNum(v1) = gs1Json.head \ "a"
            val JNum(v2) = gs2Json.head \ "a"
            
            fromJson(Stream(
              JObject(
                JField("key", keyJson.head \ "1") ::
                JField("value", JNum(v1 + v2)) :: Nil)))
          }
        }
        
        val resultJson = result.flatMap(_.toJson).copoint
        
        resultJson must haveSize(((rawData1 map { _._1 }) ++ rawData2).distinct.length)
        
        forall(resultJson) { v =>
          v must beLike {
            case obj: JObject => {
              val JNum(k) = obj \ "key"
              val JNum(v) = obj \ "value"
              
              v mustEqual (k * 2)
            }
          }
        }
      }.pendingUntilFixed
      
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
          
            val JNum(v1) = gs1Json.head \ "a"
            val JNum(v2) = gs2Json.head \ "a"
            
            fromJson(Stream(
              JObject(
                JField("key", keyJson.head \ "1") ::
                JField("value", JNum(v1 + v2)) :: Nil)))
          }
        }
        
        val resultJson = result.flatMap(_.toJson).copoint
        
        resultJson must haveSize(((rawData1 map { _._1 }) ++ rawData2).distinct.length)
        
        forall(resultJson) { v =>
          v must beLike {
            case obj: JObject => {
              val JNum(k) = obj \ "key"
              val JNum(v) = obj \ "value"
              
              v mustEqual (k * 2)
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
          
      "union" >> {
        val spec = GroupingAlignment(
          DerefObjectStatic(Leaf(Source), JPathField("2")),
          DerefObjectStatic(Leaf(Source), JPathField("2")),
          GroupingAlignment(
            DerefObjectStatic(Leaf(Source), JPathField("1")),
            DerefObjectStatic(Leaf(Source), JPathField("1")),
            fooSpec,
            barSpec, GroupingSpec.Union),
          bazSpec, GroupingSpec.Union)
          
        val forallResult = Table.merge(spec) { (key, map) =>
          val keyJson = key.toJson.copoint
          
          keyJson must not(beEmpty)
          
          val a = keyJson.head \ "1"
          val b = keyJson.head \ "2"
          
          (a mustNotEqual JNothing) or (b mustNotEqual JNothing)
          
          for {
            fooP <- map(3)
            barP <- map(4)
            bazP <- map(5)
            fooPJson <- fooP.toJson
            barPJson <- barP.toJson
            bazPJson <- bazP.toJson
          } yield {
            (fooPJson must not(beEmpty)) or
              (barPJson must not(beEmpty)) or
              (bazPJson must not(beEmpty))
            
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
        forallJson must haveSize(7)
        
        forall(forallJson) { row =>
          row must beLike {
            // 1
            case obj: JObject if (obj \ "a") == JNum(42) && (obj \ "b") == JNum(12) => {
              val JNum(ai) = obj \ "a"
              val JNum(bi) = obj \ "b"
              
              val JNum(fooi) = obj \ "foo"
              val JNum(bari) = obj \ "bar"
              val JNum(bazi) = obj \ "baz"
              
              fooi mustEqual 3
              bari mustEqual 3
              bazi mustEqual 2
            }
            
            // 2
            case obj: JObject if (obj \ "a") == JNum(7) && (obj \ "b") == JNum(42) => {
              val JNum(ai) = obj \ "a"
              val JNum(bi) = obj \ "b"
              
              val JNum(fooi) = obj \ "foo"
              val JNum(bari) = obj \ "bar"
              val JNum(bazi) = obj \ "baz"
              
              fooi mustEqual 2
              bari mustEqual 2
              bazi mustEqual 1
            }
            
            // 3
            case obj: JObject if (obj \ "a") == JNum(17) && (obj \ "b") == JNum(6) => {
              val JNum(ai) = obj \ "a"
              val JNum(bi) = obj \ "b"
              
              val JNum(fooi) = obj \ "foo"
              val JNum(bari) = obj \ "bar"
              val JNum(bazi) = obj \ "baz"
              
              fooi mustEqual 1
              bari mustEqual 1
              bazi mustEqual 1
            }
            
            // 4
            case obj: JObject if (obj \ "a") == JNum(21) && (obj \ "b") == JNum(12) => {
              val JNum(ai) = obj \ "a"
              val JNum(bi) = obj \ "b"
              
              val JNum(fooi) = obj \ "foo"
              val JNum(bari) = obj \ "bar"
              val JNum(bazi) = obj \ "baz"
              
              fooi mustEqual 1
              bari mustEqual 0
              bazi mustEqual 0
            }
            
            // 5
            case obj: JObject if (obj \ "a") == JNum(42) && (obj \ "b") == JNum(-2) => {
              val JNum(ai) = obj \ "a"
              val JNum(bi) = obj \ "b"
              
              val JNum(fooi) = obj \ "foo"
              val JNum(bari) = obj \ "bar"
              val JNum(bazi) = obj \ "baz"
              
              fooi mustEqual 1
              bari mustEqual 0
              bazi mustEqual 0
            }
            
            // 6
            case obj: JObject if (obj \ "a") == JNum(77) && (obj \ "b") == JNothing => {
              val JNum(ai) = obj \ "a"
              val JNum(bi) = obj \ "b"
              
              val JNum(fooi) = obj \ "foo"
              val JNum(bari) = obj \ "bar"
              val JNum(bazi) = obj \ "baz"
              
              fooi mustEqual 0
              bari mustEqual 1
              bazi mustEqual 0
            }
            
            // 7
            case obj: JObject if (obj \ "a") == JNothing && (obj \ "b") == JNum(1) => {
              val JNum(ai) = obj \ "a"
              val JNum(bi) = obj \ "b"
              
              val JNum(fooi) = obj \ "foo"
              val JNum(bari) = obj \ "bar"
              val JNum(bazi) = obj \ "baz"
              
              fooi mustEqual 0
              bari mustEqual 0
              bazi mustEqual 1
            }
          }
        }
      }.pendingUntilFixed
    }
  }
}
*/

object GrouperSpec extends TableModuleSpec[Free.Trampoline] with GrouperSpec[Free.Trampoline] {
  implicit def M = Trampoline.trampolineMonad

  type YggConfig = IdSourceConfig
  val yggConfig = new IdSourceConfig {
    val idSource = new IdSource {
      private val source = new java.util.concurrent.atomic.AtomicLong
      def nextId() = source.getAndIncrement
    }
  }
}
