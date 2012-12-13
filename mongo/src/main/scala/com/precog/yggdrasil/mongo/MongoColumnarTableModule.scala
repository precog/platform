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
package mongo

import com.precog.common.{MetadataStats,Path,VectorCase}
import com.precog.common.json._
import com.precog.common.security._
import com.precog.bytecode._
import com.precog.yggdrasil.jdbm3._
import com.precog.yggdrasil.util._
import com.precog.util._
import Schema._
import metadata._

import com.precog.util.{BitSet, BitSetUtil, Loop}
import com.precog.util.BitSetUtil.Implicits._

import akka.dispatch.Future

import blueeyes.json._
import blueeyes.persistence.mongo.json._
import BijectionsMongoJson._

import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;

import java.io.File
import java.util.SortedMap
import java.util.Comparator

import org.apache.jdbm.DBMaker
import org.apache.jdbm.DB

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.Ordering._
import scalaz.std.set._
import scalaz.std.list._
import scalaz.std.stream._
import scalaz.syntax.monad._
import scalaz.syntax.monoid._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.stream._
import scala.annotation.tailrec
import scala.collection.mutable

import TableModule._

trait MongoColumnarTableModuleConfig {
}

trait MongoColumnarTableModule extends BlockStoreColumnarTableModule[Future] {
  type YggConfig <: IdSourceConfig with ColumnarTableModuleConfig with BlockStoreColumnarTableModuleConfig with MongoColumnarTableModuleConfig

  trait MongoColumnarTableCompanion extends BlockStoreColumnarTableCompanion with Logging {
    def mongo: Mongo
    def dbAuthParams: Map[String, String]

    private def jTypeToProperties(tpe: JType, current: Set[String]) : Set[String] = tpe match {
      case JArrayFixedT(elements) if current.nonEmpty => elements.map {
        case (index, childType) =>
          val newPaths = current.map { s => s + "[" + index + "]" }
          jTypeToProperties(childType, newPaths)
      }.toSet.flatten

      case JObjectFixedT(fields)                      => fields.map {
        case (name, childType) => 
          val newPaths = if (current.nonEmpty) {
            current.map { s => s + "." + name }
          } else {
            Set(name)
          }
          jTypeToProperties(childType, newPaths)
      }.toSet.flatten

      case _                                          => current
    }

    def load(table: Table, apiKey: APIKey, tpe: JType): Future[Table] = {
      for {
        paths <- pathsM(table)
      } yield {
        Table(
          StreamT.unfoldM[Future, Slice, (List[Path], Option[DBCursor])]((paths.toList, None)) { 
            case (paths, Some(cursor)) => 
              M.point {
                val (slice, remainder) = makeSlice(cursor)
                Some(slice, (paths, remainder))
              }

            case (path :: xs, None) => 
              path.elements.toList match {
                case dbName :: collectionName :: Nil =>
                  M.point {
                    try {
                      val db = mongo.getDB(dbName)

                      if (! db.isAuthenticated) {
                        dbAuthParams.get(dbName).map(_.split(':')) foreach {
                          case Array(user, password) => if (! db.authenticate(user, password.toCharArray)) {
                            throw new Exception("Authentication failed for database " + dbName)
                          }
                          case invalid => throw new Exception("Invalid user:password for %s: \"%s\"".format(dbName, invalid.mkString(":")))
                        }
                      }

                      val coll = db.getCollection(collectionName)

                      val selector = jTypeToProperties(tpe, Set()).foldLeft(new BasicDBObject()) {
                        case (obj, path) => obj.append(path, 1)
                      }

                      val cursor = coll.find(new BasicDBObject(), selector)

                      val (slice, remainder) = makeSlice(cursor)
                      Some((slice, (xs, remainder)))
                    } catch {
                      case t => 
                        logger.error("Failure during Mongo query: " + t.getMessage)
                        // FIXME: We should be able to throw here and terminate the query, but something in BlueEyes is hanging when we do so
                        //throw new Exception("Failure during Mongo query: " + t.getMessage)
                        None
                    }
                  }

                case err => 
                  sys.error("MongoDB path " + path.path + " does not have the form /dbName/collectionName; rollups not yet supported.")
              }

            case (Nil, None) => 
              M.point(None)
          },
          UnknownSize
        )
      }
    }

    def makeSlice(cursor: DBCursor): (Slice, Option[DBCursor]) = {
      import TransSpecModule.paths._

      @tailrec def buildColArrays(from: DBCursor, into: Map[ColumnRef, (BitSet, Array[_])], sliceIndex: Int): (Map[ColumnRef, (BitSet, Object)], Int) = {
        if (from.hasNext && sliceIndex < yggConfig.maxSliceSize) {
          // horribly inefficient, but a place to start
          val Success(jv) = MongoToJson(from.next())
          val withIdsAndValues = jv.flattenWithPath.foldLeft(into) {
            case (acc, (jpath, JUndefined)) => acc
            case (acc, (jpath, v)) =>
              val ctype = CType.forJValue(v) getOrElse { sys.error("Cannot determine ctype for " + v + " at " + jpath + " in " + jv) }

              // The objectId becomes identity for the slices, everything else is a value
              val transformedPath = if (jpath == JPath("._id")) {
                Key \ "[0]"
              } else {
                Value \ CPath(jpath)
              }

              val ref = ColumnRef(transformedPath, ctype)

              val pair: (BitSet, Array[_]) = v match {
                case JBool(b) => 
                  val (defined, col) = acc.getOrElse(ref, (new BitSet, new Array[Boolean](yggConfig.maxSliceSize))).asInstanceOf[(BitSet, Array[Boolean])]
                  col(sliceIndex) = b
                  (defined + sliceIndex, col)
                  
                case JNum(d) => {
                  val isLong = ctype == CLong
                  val isDouble = ctype == CDouble
                  
                  val (defined, col) = if (isLong) {
                    val (defined, col) = acc.getOrElse(ref, (new BitSet, new Array[Long](yggConfig.maxSliceSize))).asInstanceOf[(BitSet, Array[Long])]
                    col(sliceIndex) = d.toLong
                    (defined, col)
                  } else if (isDouble) {
                    val (defined, col) = acc.getOrElse(ref, (new BitSet, new Array[Double](yggConfig.maxSliceSize))).asInstanceOf[(BitSet, Array[Double])]
                    col(sliceIndex) = d.toDouble
                    (defined, col)
                  } else {
                    val (defined, col) = acc.getOrElse(ref, (new BitSet, new Array[BigDecimal](yggConfig.maxSliceSize))).asInstanceOf[(BitSet, Array[BigDecimal])]
                    col(sliceIndex) = d
                    (defined, col)
                  }
                  
                  (defined + sliceIndex, col)
                }

                case JString(s) => 
                  val (defined, col) = acc.getOrElse(ref, (new BitSet, new Array[String](yggConfig.maxSliceSize))).asInstanceOf[(BitSet, Array[String])]
                  col(sliceIndex) = s
                  (defined + sliceIndex, col)
                
                case JArray(Nil)  => 
                  val (defined, col) = acc.getOrElse(ref, (new BitSet, null)).asInstanceOf[(BitSet, Array[Boolean])]
                  (defined + sliceIndex, col)

                case JObject(values) if values.isEmpty => 
                  val (defined, col) = acc.getOrElse(ref, (new BitSet, null)).asInstanceOf[(BitSet, Array[Boolean])]
                  (defined + sliceIndex, col)

                case JNull        => 
                  val (defined, col) = acc.getOrElse(ref, (new BitSet, null)).asInstanceOf[(BitSet, Array[Boolean])]
                  (defined + sliceIndex, col)
              }

              acc + (ref -> pair)
          }

          //println("Computed " + withIdsAndValues)

          buildColArrays(from, withIdsAndValues, sliceIndex + 1)
        } else {
          (into, sliceIndex)
        }
      }
  
      // FIXME: If cursor is empty the generated
      // columns won't satisfy sampleData.schema. This will cause the subsumption test in
      // Slice#typed to fail unless it allows for vacuous success
      val slice = new Slice {
        val (cols, size) = buildColArrays(cursor, Map.empty[ColumnRef, (BitSet, Array[_])], 0) 
        val columns = cols map {
          case (ref @ ColumnRef(_, CBoolean), (defined, values))     => (ref, ArrayBoolColumn(defined, values.asInstanceOf[Array[Boolean]]))
          case (ref @ ColumnRef(_, CLong), (defined, values))        => (ref, ArrayLongColumn(defined, values.asInstanceOf[Array[Long]]))
          case (ref @ ColumnRef(_, CDouble), (defined, values))      => (ref, ArrayDoubleColumn(defined, values.asInstanceOf[Array[Double]]))
          case (ref @ ColumnRef(_, CNum), (defined, values))         => (ref, ArrayNumColumn(defined, values.asInstanceOf[Array[BigDecimal]]))
          case (ref @ ColumnRef(_, CString), (defined, values))      => (ref, ArrayStrColumn(defined, values.asInstanceOf[Array[String]]))
          case (ref @ ColumnRef(_, CEmptyArray), (defined, values))  => (ref, new BitsetColumn(defined) with EmptyArrayColumn)
          case (ref @ ColumnRef(_, CEmptyObject), (defined, values)) => (ref, new BitsetColumn(defined) with EmptyObjectColumn)
          case (ref @ ColumnRef(_, CNull), (defined, values))        => (ref, new BitsetColumn(defined) with NullColumn)
        }
      }
  
      (slice, if (cursor.hasNext) Some(cursor) else { cursor.close(); None })
    }
  }
}


// vim: set ts=4 sw=4 et:
