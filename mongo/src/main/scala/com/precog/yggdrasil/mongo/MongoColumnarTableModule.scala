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

  def includeIdField: Boolean

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

    sealed trait LoadState
    case class InitialLoad(paths: List[Path]) extends LoadState
    case class InLoad(cursorGen: () => DBCursor, skip: Int, remainingPaths: List[Path]) extends LoadState

    def safeOp[A](nullMessage: String)(v: => A): Option[A] = try {
      Option(v) orElse { logger.error(nullMessage); None }
    } catch {
      case t: Throwable => logger.error("Failure during Mongo query: %s(%s)".format(t.getClass, t.getMessage)); None
    }

    def load(table: Table, apiKey: APIKey, tpe: JType): Future[Table] = {
      for {
        paths <- pathsM(table)
      } yield {
        Table(
          StreamT.unfoldM[Future, Slice, LoadState](InitialLoad(paths.toList)) { 
            case InLoad(cursorGen, skip, remaining) => 
              logger.trace("Running InLoad")
              M.point {
                val (slice, nextSkip) = makeSlice(cursorGen, skip)
                Some(slice, nextSkip.map(InLoad(cursorGen, _, remaining)).getOrElse(InitialLoad(remaining)))
              }

            case InitialLoad(path :: xs) => 
              path.elements.toList match {
                case dbName :: collectionName :: Nil =>
                  logger.trace("Running InitialLoad")
                  M.point {
                    for {
                      db <- safeOp("Database " + dbName + " does not exist")(mongo.getDB(dbName)).flatMap { d =>
                        if (! d.isAuthenticated && dbAuthParams.contains(dbName)) {                          
                          logger.trace("Running auth setup for " + dbName)
                          dbAuthParams.get(dbName).map(_.split(':')) flatMap {
                            case Array(user, password) =>
                              if (d.authenticate(user, password.toCharArray)) {
                                Some(d)
                              } else {
                                logger.error("Authentication failed for database " + dbName); None
                              }

                            case invalid =>
                              logger.error("Invalid user:password for %s: \"%s\"".format(dbName, invalid.mkString(":"))); None
                          }
                        } else {
                          Some(d)
                        }
                      }
                      coll <- safeOp("Collection " + collectionName + " does not exist") {
                        logger.trace("Fetching collection: " + collectionName)
                        db.getCollection(collectionName)
                      }
                      slice <- safeOp("Invalid result in query") {
                        logger.trace("Getting data from " + coll)
                        val selector = jTypeToProperties(tpe, Set()).foldLeft(new BasicDBObject()) {
                          case (obj, path) => obj.append(path, 1)
                        }

                        val cursorGen = () => coll.find(new BasicDBObject(), selector)

                        val (slice, nextSkip) = makeSlice(cursorGen, 0)

                        logger.debug("Gen slice of size " + slice.size)
                        (slice, nextSkip.map(InLoad(cursorGen, _, xs)).getOrElse(InitialLoad(xs)))
                      }
                    } yield slice
                  }

                case err => 
                  sys.error("MongoDB path " + path.path + " does not have the form /dbName/collectionName; rollups not yet supported.")
              }

            case InitialLoad(Nil) =>
              M.point(None)
          },
          UnknownSize
        )
      }
    }

    def makeSlice(cursorGen: () => DBCursor, skip: Int): (Slice, Option[Int]) = {
      import TransSpecModule.paths._

      val idPath: CPath = Key \ 0

      // Sort by _id always to mimic JDBM
      val cursor = cursorGen().sort(new BasicDBObject("_id", 1))skip(skip)

      @tailrec def buildColArrays(from: DBCursor, into: Map[ColumnRef, ArrayColumn[_]], sliceIndex: Int): (Map[ColumnRef, ArrayColumn[_]], Int) = {
        if (sliceIndex < yggConfig.maxSliceSize && from.hasNext) {
          // horribly inefficient, but a place to start
          val Success(jv) = MongoToJson(from.next())
          val refs = withIdsAndValues(jv, into, sliceIndex, yggConfig.maxSliceSize, Some({
            jpath => if (jpath == JPath("._id")) {
              idPath
            } else {
              Value \ CPath(jpath)
            }
          }))
          buildColArrays(from, refs, sliceIndex + 1)
        } else {
          (into, sliceIndex)
        }
      }
  
      // FIXME: If cursor is empty the generated
      // columns won't satisfy sampleData.schema. This will cause the subsumption test in
      // Slice#typed to fail unless it allows for vacuous success
      val slice = new Slice {
        val (cols, size) = buildColArrays(cursor, Map.empty[ColumnRef, ArrayColumn[_]], 0)
        val columns = cols.flatMap {
          // The identity column get duped to provide for both identity and an "_id" value
          case (ref @ ColumnRef(`idPath`, CString), column)      =>
            if (includeIdField) {
              Seq(ColumnRef(Value \ CPath("._id"), CString) -> column,
                  ref -> column)
            } else {
              Seq(ref -> column)
            }
          case nonId => Seq(nonId)
        }
      }

      val nextSkip = if (cursor.hasNext) {
        Some(skip + slice.size)
      } else {
        None
      }

      cursor.close()

      (slice, nextSkip)
    }
  }
}


// vim: set ts=4 sw=4 et:
