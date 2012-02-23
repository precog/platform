package com.precog
package daze

import java.io._
import java.util.zip._

import scala.annotation.tailrec

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.syntax.monad._
import Iteratee._

import com.precog.common._

trait FileSerialization[E] {
  def iStream(file: File) = new DataInputStream(new GZIPInputStream(new FileInputStream(file)))
  def oStream(file: File) = new DataOutputStream(new GZIPOutputStream(new FileOutputStream(file)))

  def writeEvents(out: DataOutputStream, ev: Vector[E]): IO[Unit]
  def readEvents(in: DataInputStream): IO[Option[Vector[E]]]

  def reader[X](file: File): EnumeratorP[X, Vector[E], IO] = new EnumeratorP[X, Vector[E], IO] {
    def apply[G[_]](implicit MO: G |>=| IO): EnumeratorT[X, Vector[E], G] = {
      import MO._
      new EnumeratorT[X, Vector[E], G] {
        def apply[A] = {
          def step(in: DataInputStream): StepT[X, Vector[E], G, A] => IterateeT[X, Vector[E], G, A] = s => s mapContOr(
            k => iterateeT(MO.promote(readEvents(in)) flatMap {
                   case Some(ev) => (k(elInput(ev)) >>== step(in)).value
                   case None     => (k(eofInput) >>== step(in)).value
                 }), 
            iterateeT(MO.promote(IO(in.close)) >> s.pointI.value)
          )

          (s: StepT[X, Vector[E], G, A]) => step(iStream(file))(s)
        }
      }
    }
  }

  def writer[X, F[_]](file: File)(implicit MO: F |>=| IO): IterateeT[X, Vector[E], F, File] = {
    import MO._
    def dump(outIO: F[DataOutputStream]): IterateeT[X, Vector[E], F, File] = 
      cont(
        (_: Input[Vector[E]]).fold(
          el    = el => dump(for { out  <- outIO; _ <- MO.promote(writeEvents(out, el)) } yield out),
          empty = dump(outIO),
          eof   = iterateeT(
            for {
              out <- outIO
              _   <- MO.promote(IO(out.close))
            } yield sdone[X, Vector[E], F, File](file, eofInput))))

    dump(MO.promote(IO(oStream(file))))
  }
}

import yggdrasil._
object SimpleProjectionSerialization extends FileSerialization[SEvent] {
  import blueeyes.json._
  import blueeyes.json.JsonDSL._

  val chunkSize = 2000 // Needs to be configurable

  def writeEvents(out: DataOutputStream, ev: Vector[SEvent]): IO[Unit] = IO { 
    ev foreach { 
      case (ids, sv) => 
        out.writeInt(ids.size)
        for (id <- ids) out.writeLong(id)
        val jvStr = compact(render(sv.toJValue))
        out.writeUTF(jvStr)
    } 
  }

  def readEvents(in: DataInputStream): IO[Option[Vector[SEvent]]] = IO {
    @tailrec
    def readOne(acc: Vector[SEvent], remaining: Int): Vector[SEvent] = {
      if (remaining > 0) { 
        val (newAcc,newRem) = try {
          val idCount = in.readInt
          val ids = VectorCase((0 until idCount).map(_ => in.readLong): _*)
          val jstr = in.readUTF
          val ev = (ids, SValue.fromJValue(JsonParser.parse(jstr)))
          (acc :+ ev, remaining - 1)
        } catch {
          case ex: EOFException => (acc, 0)
        }

        readOne(newAcc, newRem)
      } else {
        acc
      }
    }

    val slurped = readOne(Vector(), chunkSize)

    if (slurped.size == 0) None else Some(slurped)
  }
}
