package com.precog.yggdrasil
package table

import com.precog.bytecode._
import com.precog.common._

import blueeyes.json._
import blueeyes.json.JsonAST._

import scala.annotation.tailrec
import scala.util.Random
import scalaz._
import scalaz.effect._
import scalaz.std.list._
import scalaz.syntax.copointed._

import org.specs2.ScalaCheck
import org.specs2.mutable._

import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import SampleData._

trait BlockLoadTestSupport[M[+_]] extends TestColumnarTableModule[M] with StubStorageModule[M] with TableModuleTestSupport[M] {
  type Key = JArray
  case class Projection(descriptor: ProjectionDescriptor, data: Stream[JValue]) extends BlockProjectionLike[JArray, Slice] {
    val slices = fromJson(data).slices.toStream.copoint

    def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit] = IO(sys.error("Insert not supported."))

    implicit val keyOrder: Order[JArray] = Order[List[JValue]].contramap((_: JArray).elements)

    def getBlockAfter(id: Option[JArray], colSelection: Set[ColumnDescriptor] = Set()): Option[BlockData] = {
      @tailrec def findBlockAfter(id: JArray, blocks: Stream[Slice]): Option[Slice] = {
        blocks match {
          case x #:: xs =>
            if ((x.toJson(x.size - 1) \ "key") == id) xs.headOption else findBlockAfter(id, xs)

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
              colSelection.exists { desc => desc.selector == jpath && desc.valueType == ctype }
          }
        }

        BlockData(s0.toJson(0) \ "key" --> classOf[JArray], s0.toJson(s0.size - 1) \ "key" --> classOf[JArray], s0) 
      }
    }
  }
}


trait BlockLoadSpec[M[+_]] extends Specification with ScalaCheck { self =>
  implicit def M: Monad[M]
  implicit def coM: Copointed[M]

  def checkLoadDense = {
    implicit val gen = sample(objectSchema(_, 3))
    check { (sample: SampleData) =>
      forall(sample.schema) { case (idCount, schema) =>
        val module = new BlockLoadTestSupport[M] with BlockStoreColumnarTableModule[M] {
          def M = self.M
          def coM = self.coM

          val projections = {
            schema.grouped(2) map { subschema =>
              val descriptor = ProjectionDescriptor(
                idCount, 
                subschema map {
                  case (jpath, ctype) => ColumnDescriptor(Path("/test"), jpath, ctype, Authorities.None)
                } toList
              )

              descriptor -> Projection( 
                descriptor, 
                sample.data map { jv =>
                  subschema.foldLeft[JValue](JObject(Nil)) {
                    case (obj, (jpath, _)) => obj.set(jpath, jv.get(JPath(JPathField("value") :: jpath.nodes)))
                  }
                }
              )
            } toMap
          }

          object storage extends Storage
        }

        module.toJson(module.ops.constString(Set(CString("/test"))).load("", Schema.mkType(schema).get).copoint) must_== sample.data
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
