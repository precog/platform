package com.precog.yggdrasil
package table

import com.precog.common.Path
import com.precog.common.VectorCase
import com.precog.bytecode.JType
import com.precog.yggdrasil.util._

import akka.actor.ActorSystem
import akka.dispatch._
import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import com.weiglewilczek.slf4s.Logging

import scala.annotation.tailrec
import scala.collection.BitSet
import scala.collection.mutable.LinkedHashSet
import scala.util.Random

import scalaz._
import scalaz.effect.IO 
import scalaz.syntax.copointed._
import scalaz.std.anyVal._
import scalaz.std.list._

import org.specs2._
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

import TableModule._

trait BlockStoreTestSupport[M[+_]] { self =>
  implicit def M: Monad[M] with Copointed[M]

  abstract class BlockStoreTestModule extends 
      BlockStoreColumnarTableModule[M] with
      ColumnarTableModuleTestSupport[M] with 
      StubStorageModule[M] {

    import trans._
    import CValueGenerators._

    type YggConfig = IdSourceConfig
    type Key = JArray
    type GroupId = Int

    implicit def M = self.M

    object storage extends Storage

    case class Projection(descriptor: ProjectionDescriptor, data: Stream[JValue]) extends BlockProjectionLike[JArray, Slice] {
      val slices = fromJson(data).slices.toStream.copoint

      def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit] = IO(sys.error("Insert not supported."))

      implicit val keyOrder: Order[JArray] = Order[List[JValue]].contramap((_: JArray).elements)

      def getBlockAfter(id: Option[JArray], colSelection: Set[ColumnDescriptor] = Set()): Option[BlockProjectionData[JArray, Slice]] = {
        @tailrec def findBlockAfter(id: JArray, blocks: Stream[Slice]): Option[Slice] = {
          blocks.filterNot(_.isEmpty) match {
            case x #:: xs =>
              if ((x.toJson(x.size - 1).getOrElse(JNothing) \ "key") > id) Some(x) else findBlockAfter(id, xs)

            case _ => None
          }
        }

        val slice = id map { key =>
          findBlockAfter(key, slices)
        } getOrElse {
          slices.headOption
        }
        
        slice map { s => 
          val s0 = new Slice {
            val size = s.size
            val columns = s.columns filter {
              case (ColumnRef(jpath, ctype), _) =>
                colSelection.isEmpty || 
                jpath.nodes.head == JPathField("key") ||
                colSelection.exists { desc => (JPathField("value") \ desc.selector) == jpath && desc.valueType == ctype }
            }
          }

          BlockProjectionData[JArray, Slice](s0.toJson(0).getOrElse(JNothing) \ "key" --> classOf[JArray], s0.toJson(s0.size - 1).getOrElse(JNothing) \ "key" --> classOf[JArray], s0)
        }
      }
    }

    trait TableCompanion extends BlockStoreColumnarTableCompanion {
      implicit val geq: scalaz.Equal[Int] = intInstance
    }

    object Table extends TableCompanion
    
    val yggConfig = new IdSourceConfig {
      val idSource = new IdSource {
        private val source = new java.util.concurrent.atomic.AtomicLong
        def nextId() = source.getAndIncrement
      }
    }
    
    def compliesWithSchema(jv: JValue, ctype: CType): Boolean = (jv, ctype) match {
      case (_: JNum, CNum | CLong | CDouble) => true
      case (JNothing, CUndefined) => true
      case (JNull, CNull) => true
      case (_: JBool, CBoolean) => true
      case (_: JString, CString) => true
      case (JObject(Nil), CEmptyObject) => true
      case (JArray(Nil), CEmptyArray) => true
      case _ => false
    }

    def sortTransspec(sortKeys: JPath*): TransSpec1 = InnerObjectConcat(sortKeys.zipWithIndex.map {
      case (sortKey, idx) => WrapObject(
        sortKey.nodes.foldLeft[TransSpec1](DerefObjectStatic(Leaf(Source), JPathField("value"))) {
          case (innerSpec, field: JPathField) => DerefObjectStatic(innerSpec, field)
          case (innerSpec, index: JPathIndex) => DerefArrayStatic(innerSpec, index)
        },
        "%09d".format(idx)
      )
    }: _*)
  }
}


// vim: set ts=4 sw=4 et:
