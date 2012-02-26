package com.precog
package yggdrasil

import java.io._
import java.util.zip._

import com.precog.common.VectorCase

import scala.annotation.tailrec

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.syntax.monad._
import Iteratee._

trait FileSerialization[E] {
  def iStream(file: File) = new DataInputStream(new GZIPInputStream(new FileInputStream(file)))
  def oStream(file: File) = new DataOutputStream(new GZIPOutputStream(new FileOutputStream(file)))

  def writeElement(out: DataOutputStream, ev: E): IO[Unit]
  def readElement(in: DataInputStream): IO[Option[E]]

  def reader[X](file: File): EnumeratorP[X, E, IO] = new EnumeratorP[X, E, IO] {
    def apply[G[_]](implicit MO: G |>=| IO): EnumeratorT[X, E, G] = {
      import MO._
      new EnumeratorT[X, E, G] {
        def apply[A] = {
          def step(in: DataInputStream): StepT[X, E, G, A] => IterateeT[X, E, G, A] = s => s mapContOr(
            k => iterateeT(MO.promote(readElement(in)) flatMap {
                   case Some(ev) => (k(elInput(ev)) >>== step(in)).value
                   case None     => (k(eofInput) >>== step(in)).value
                 }), 
            iterateeT(MO.promote(IO(in.close)) >> s.pointI.value)
          )

          (s: StepT[X, E, G, A]) => step(iStream(file))(s)
        }
      }
    }
  }

  def writer[X, F[_]](file: File)(implicit MO: F |>=| IO): IterateeT[X, E, F, File] = {
    import MO._
    def dump(outIO: F[DataOutputStream]): IterateeT[X, E, F, File] = 
      cont(
        (_: Input[E]).fold(
          el    = el => dump(for { out  <- outIO; _ <- MO.promote(writeElement(out, el)) } yield out),
          empty = dump(outIO),
          eof   = iterateeT(
            for {
              out <- outIO
              _   <- MO.promote(IO(out.close))
            } yield sdone[X, E, F, File](file, eofInput))))

    dump(MO.promote(IO(oStream(file))))
  }
}

object FileSerialization {
  def noop[E]: FileSerialization[E] = new FileSerialization[E] {
    def writeElement(out: DataOutputStream, ev: E): IO[Unit] = IO(())
    def readElement(in: DataInputStream): IO[Option[E]] = IO(Option.empty[E])
  }
}

import yggdrasil._
object SimpleProjectionSerialization extends FileSerialization[Vector[SEvent]] {
  import blueeyes.json._
  import blueeyes.json.JsonDSL._

  private def writeEvent(out: DataOutputStream, ids: Identities, sv: SValue) = {
    out.writeInt(ids.size)
    for (id <- ids) out.writeLong(id)
    val jvStr = compact(render(sv.toJValue))
    out.writeUTF(jvStr)
  }

  def writeElement(out: DataOutputStream, v: Vector[SEvent]): IO[Unit] = IO { 
    out.writeInt(v.size)
    for ((ids, sv) <- v) writeEvent(out, ids, sv)
  }

  private def readEvent(in: DataInputStream): SEvent = {
    val idCount = in.readInt()
    val ids = VectorCase.fromSeq((0 until idCount).map(_ => in.readLong))
    val jstr = in.readUTF
    (ids, SValue.fromJValue(JsonParser.parse(jstr)))
  }

  def readElement(in: DataInputStream): IO[Option[Vector[SEvent]]] = IO {
    @tailrec
    def readChunk(acc: Vector[SEvent], remaining: Int): Vector[SEvent] = {
      if (remaining > 0) readChunk(acc :+ readEvent(in), remaining - 1)
      else               acc
    }

    val slurped = readChunk(Vector(), in.readInt())
    if (slurped.size == 0) None else Some(slurped)
  }
}
