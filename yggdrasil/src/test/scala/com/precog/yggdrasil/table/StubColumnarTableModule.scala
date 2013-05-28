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

import vfs.ResourceError
import com.precog.bytecode.JType
import com.precog.common.Path
import com.precog.util.VectorCase
import com.precog.common._
import com.precog.common.security._

import akka.actor.ActorSystem

import blueeyes.json._

import scala.annotation.tailrec

import scalaz._
import scalaz.Validation._
import scalaz.std.stream._
import scalaz.syntax.comonad._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._
import scalaz.std.anyVal._

import TableModule._

trait StubColumnarTableModule[M[+_]] extends ColumnarTableModuleTestSupport[M] {
  import trans._

  implicit def M: Monad[M] with Comonad[M]
  
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
    
    override def load(apiKey: APIKey, jtpe: JType) = EitherT {
      self.toJson map { events =>
        val parsedV = events.toStream.traverse[({ type λ[α] = Validation[ResourceError, α] })#λ, Stream[JObject]] {
          case JString(pathStr) => success {
            indexLock synchronized {      // block the WHOLE WORLD
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
          }

          case x => 
            failure(ResourceError.corrupt("Attempted to load JSON as a table from something that wasn't a string: " + x))
        }

        parsedV.map(_.flatten).disjunction.map(fromJson(_))
      }
    }

    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder = SortAscending, unique: Boolean = false): M[Seq[Table]] = sys.error("todo")

    override def toString = toStrings.copoint.mkString("\n")
  }
}

