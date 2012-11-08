package com.precog.yggdrasil
package table

import com.precog.common.json._
import com.precog.bytecode.JType
import com.precog.common.Path
import com.precog.common.VectorCase

import akka.actor.ActorSystem

import blueeyes.json._

import scala.annotation.tailrec

import scalaz._
import scalaz.syntax.copointed._
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._
import scalaz.std.anyVal._

import TableModule._

trait StubColumnarTableModule[M[+_]] extends ColumnarTableModuleTestSupport[M] {
  import trans._

  implicit def M: Monad[M] with Copointed[M]
  
  private var initialIndices = collection.mutable.Map[Path, Int]()    // if we were doing this for real: j.u.c.HashMap
  private var currentIndex = 0                                        // if we were doing this for real: j.u.c.a.AtomicInteger
  private val indexLock = new AnyRef                                  // if we were doing this for real: DIE IN A FIRE!!!

  trait TableCompanion extends ColumnarTableCompanion {
    def apply(slices: StreamT[M, Slice], size: TableSize): Table = new Table(slices, size)
    def align(sourceLeft: Table, alignOnL: TransSpec1, sourceRight: Table, alignOnR: TransSpec1): M[(Table, Table)] = sys.error("todo")
  }

  class Table(slices: StreamT[M, Slice], size: TableSize) extends ColumnarTable(slices, size) { self: Table => 
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean = false): M[Table] = {
      // We use the sort transspec1 to compute a new table with a combination of the 
      // original data and the new sort columns, referenced under the sortkey namespace
      val tableWithSortKey = transform(InnerObjectConcat(WrapObject(sortKey, "0"),
                                                         WrapObject(Leaf(Source), "1")))

      implicit val jValueOrdering = if (sortOrder.isAscending) {
        JValue.order.toScalaOrdering
      } else {
        JValue.order.toScalaOrdering.reverse
      }

      tableWithSortKey.toJson.map {
        jvals =>
          fromJson(jvals.toList.sortBy(_ \ "0").toStream)
      }.map(_.transform(DerefObjectStatic(Leaf(Source), CPathField("1"))))
    }
    
    override def load(uid: UserId, jtpe: JType) = {
      self.toJson map { events =>
        fromJson {
          events.toStream flatMap {
            case JString(pathStr) => indexLock synchronized {      // block the WHOLE WORLD
              val path = Path(pathStr)
        
              val index = initialIndices get path getOrElse {
                initialIndices += (path -> currentIndex)
                currentIndex
              }
              
              val target = path.path.replaceAll("/$", ".json")
              val src = io.Source fromInputStream getClass.getResourceAsStream(target)
              val parsed = src.getLines map JParser.parse toStream
              
              currentIndex += parsed.length
              
              parsed zip (Stream from index) map {
                case (value, id) => JObject(JField("key", JArray(JNum(id) :: Nil)) :: JField("value", value) :: Nil)
              }
            }

            case x => sys.error("Attempted to load JSON as a table from something that wasn't a string: " + x)
          }
        }
      }
    }

    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder = SortAscending, unique: Boolean = false): M[Seq[Table]] = sys.error("todo")

    override def toString = toStrings.copoint.mkString("\n")
  }
}

