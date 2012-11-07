package com.precog.yggdrasil
package table

import com.precog.bytecode.JType
import com.precog.common.Path
import com.precog.common.json._
import com.precog.common.VectorCase
import com.precog.util.PrecogUnit
import com.precog.yggdrasil.util._

import akka.actor.ActorSystem
import akka.dispatch._
import blueeyes.json._
import com.weiglewilczek.slf4s.Logging

import scala.annotation.tailrec
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


trait BlockStoreTestModule[M[+_]] extends BaseBlockStoreTestModule[M] {
  type GroupId = String
  private val groupId = new java.util.concurrent.atomic.AtomicInteger
  def newGroupId = "groupId(" + groupId.getAndIncrement + ")"

  class YggConfig extends IdSourceConfig with BlockStoreColumnarTableModuleConfig with ColumnarTableModuleConfig {
    val idSource = new IdSource {
      private val source = new java.util.concurrent.atomic.AtomicLong
      def nextId() = source.getAndIncrement
    }

    val maxSliceSize = 10
  }

  val yggConfig = new YggConfig
}

trait BaseBlockStoreTestModule[M[+_]] extends 
  BlockStoreColumnarTableModule[M] with
  ColumnarTableModuleTestSupport[M] with 
  StubStorageModule[M] {

    import trans._
    import CValueGenerators._

    type Key = JArray

    implicit def M: Monad[M] with Copointed[M]

    object storage extends Storage

    case class Projection(descriptor: ProjectionDescriptor, data: Stream[JValue]) extends BlockProjectionLike[JArray, Slice] {
      val slices = fromJson(data).slices.toStream.copoint

      def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): Unit = sys.error("Insert not supported.")
      def commit(): IO[PrecogUnit] = sys.error("Commit not supported.")

      implicit val keyOrder: Order[JArray] = Order[List[JValue]].contramap((_: JArray).elements)

      def getBlockAfter(id: Option[JArray], colSelection: Set[ColumnDescriptor] = Set()): Option[BlockProjectionData[JArray, Slice]] = {
        @tailrec def findBlockAfter(id: JArray, blocks: Stream[Slice]): Option[Slice] = {
          blocks.filterNot(_.isEmpty) match {
            case x #:: xs =>
              if ((x.toJson(x.size - 1).getOrElse(JUndefined) \ "key") > id) Some(x) else findBlockAfter(id, xs)

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
                jpath.nodes.head == CPathField("key") ||
                colSelection.exists { desc => (CPathField("value") \ desc.selector) == jpath && desc.valueType == ctype }
            }
          }

          BlockProjectionData[JArray, Slice](s0.toJson(0).getOrElse(JUndefined) \ "key" --> classOf[JArray], s0.toJson(s0.size - 1).getOrElse(JUndefined) \ "key" --> classOf[JArray], s0)
        }
      }
    }

    trait TableCompanion extends BlockStoreColumnarTableCompanion {
      implicit val geq: scalaz.Equal[Int] = intInstance
    }

    object Table extends TableCompanion

    def compliesWithSchema(jv: JValue, ctype: CType): Boolean = (jv, ctype) match {
      case (_: JNum, CNum | CLong | CDouble) => true
      case (JUndefined, CUndefined) => true
      case (JNull, CNull) => true
      case (_: JBool, CBoolean) => true
      case (_: JString, CString) => true
      case (JObject(fields), CEmptyObject) if fields.isEmpty => true
      case (JArray(Nil), CEmptyArray) => true
      case _ => false
    }

    def sortTransspec(sortKeys: CPath*): TransSpec1 = InnerObjectConcat(sortKeys.zipWithIndex.map {
      case (sortKey, idx) => WrapObject(
        sortKey.nodes.foldLeft[TransSpec1](DerefObjectStatic(Leaf(Source), CPathField("value"))) {
          case (innerSpec, field: CPathField) => DerefObjectStatic(innerSpec, field)
          case (innerSpec, index: CPathIndex) => DerefArrayStatic(innerSpec, index)
        },
        "%09d".format(idx)
      )
    }: _*)
}

object BlockStoreTestModule {
  def empty[M[+_]](implicit M0: Monad[M] with Copointed[M]) = new BlockStoreTestModule[M] {
    val M = M0 
    val projections = Map.empty[ProjectionDescriptor, Projection]
  }
}

trait BlockStoreTestSupport[M[+_]] { self =>
  implicit def M: Monad[M] with Copointed[M]

  def emptyTestModule = BlockStoreTestModule.empty[M]
}


// vim: set ts=4 sw=4 et:
