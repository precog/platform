package com.precog
package daze

import java.io._
import java.util.zip._

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.syntax.monad._
import Iteratee._

trait FileSerialization[E] {
  def iStream(file: File) = new DataInputStream(new GZIPInputStream(new FileInputStream(file)))
  def oStream(file: File) = new DataOutputStream(new GZIPOutputStream(new FileOutputStream(file)))

  def writeEvent(out: DataOutputStream, ev: E): IO[Unit]
  def readEvent(in: DataInputStream): IO[Option[E]]

  def reader[X](file: File): EnumeratorP[X, E, IO] = new EnumeratorP[X, E, IO] {
    def apply[G[_]](implicit MO: G |>=| IO): EnumeratorT[X, E, G] = {
      import MO._
      new EnumeratorT[X, E, G] {
        def apply[A] = {
          def step(in: DataInputStream): StepT[X, E, G, A] => IterateeT[X, E, G, A] = s => s mapContOr(
            k => iterateeT(MO.promote(readEvent(in)) flatMap {
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
          el    = el => dump(for { out  <- outIO; _ <- MO.promote(writeEvent(out, el)) } yield out),
          empty = dump(outIO),
          eof   = iterateeT(
            for {
              out <- outIO
              _   <- MO.promote(IO(out.close))
            } yield sdone[X, E, F, File](file, eofInput))))

    dump(MO.promote(IO(oStream(file))))
  }
}

import yggdrasil._
object SimpleProjectionSerialization extends FileSerialization[SEvent] {
  import blueeyes.json._
  import blueeyes.json.JsonDSL._

  def writeEvent(out: DataOutputStream, ev: SEvent): IO[Unit] = IO { 
    ev match {
      case (ids, sv) => 
        out.writeInt(ids.size)
        for (id <- ids) out.writeLong(id)
        val jvStr = compact(render(sv.toJValue))
        out.writeUTF(jvStr)
    }
  }

  def readEvent(in: DataInputStream): IO[Option[SEvent]] = IO {
    try {
      val idCount = in.readInt
      val ids = (0 until idCount).map(_ => in.readLong).toList
      val jstr = in.readUTF
      Some((ids, SValue.fromJValue(JsonParser.parse(jstr))))
    } catch {
      case ex: EOFException => None
    }
  }
}
