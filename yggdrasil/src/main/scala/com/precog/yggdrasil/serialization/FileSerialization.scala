package com.precog.yggdrasil
package serialization

import java.io._
import java.util.zip._
import scala.annotation.tailrec
import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.syntax.monad._
import Iteratee._


trait FileSerialization[E] extends StreamSerialization {
  def writeElement(out: DataOutputStream, ev: E): IO[Unit]
  def readElement(in: DataInputStream): IO[Option[E]]
}

object FileSerialization {
  def noop[E]: FileSerialization[E] = new FileSerialization[E] with ZippedStreamSerialization {
    def writeElement(out: DataOutputStream, ev: E): IO[Unit] = IO(())
    def readElement(in: DataInputStream): IO[Option[E]] = IO(Option.empty[E])
  }
}

trait IterateeFileSerialization[E] extends FileSerialization[E] {
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

