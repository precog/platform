package com.precog.yggdrasil
package table

import com.precog.common.json._
import com.precog.bytecode.JType
import com.precog.common.Path
import com.precog.common.VectorCase

import akka.actor.ActorSystem

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser

import scala.annotation.tailrec
import scala.collection.BitSet

import scalaz._
import scalaz.syntax.copointed._
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._
import scalaz.std.anyVal._

import TableModule._

trait StubColumnarTableModule[M[+_]] extends ColumnarTableModuleTestSupport[M] {
  import trans._

  implicit def M: Monad[M] with Copointed[M]
  
  class MemoContext extends MemoizationContext {
    import trans._
    
    def memoize(table: Table, memoId: MemoId): M[Table] = M.point(table)
    def sort(table: Table, sortKey: TransSpec1, sortOrder: DesiredSortOrder, memoId: MemoId, unique: Boolean = true): M[Table] =
      table.sort(sortKey, sortOrder)
    
    def expire(memoId: MemoId): Unit = ()
    def purge(): Unit = ()
  }

  def newMemoContext = new MemoContext

  private var initialIndices = collection.mutable.Map[Path, Int]()    // if we were doing this for real: j.u.c.HashMap
  private var currentIndex = 0                                        // if we were doing this for real: j.u.c.a.AtomicInteger
  private val indexLock = new AnyRef                                  // if we were doing this for real: DIE IN A FIRE!!!

  trait TableCompanion extends ColumnarTableCompanion {
    def apply(slices: StreamT[M, Slice]): Table = new Table(slices)
    def align(sourceLeft: Table, alignOnL: TransSpec1, sourceRight: Table, alignOnR: TransSpec1): M[(Table, Table)] = sys.error("todo")
  }

  class Table(slices: StreamT[M, Slice]) extends ColumnarTable(slices) { self: Table => 
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean = true): M[Table] = {
      // We use the sort transspec1 to compute a new table with a combination of the 
      // original data and the new sort columns, referenced under the sortkey namespace
      val tableWithSortKey = transform(InnerObjectConcat(Leaf(Source), WrapObject(sortKey, TableModule.paths.SortKey.name)))

      implicit val jValueOrdering = blueeyes.json.xschema.DefaultOrderings.JValueOrdering

      tableWithSortKey.toJson.map {
        jvals => fromJson(jvals.toList.sorted.toStream)
      }
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
              val parsed = src.getLines map JsonParser.parse toStream
              
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

    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder = SortAscending, unique: Boolean = true): M[Seq[Table]] = sys.error("todo")

    override def toString = toStrings.copoint.mkString("\n")
  }
}

