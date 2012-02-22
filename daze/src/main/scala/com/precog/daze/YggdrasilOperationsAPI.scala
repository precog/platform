package com.precog.daze

import com.precog.yggdrasil._
import com.precog.yggdrasil.shard._
import com.precog.yggdrasil.util._
import com.precog.util._

import akka.dispatch.Await
import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.util.Duration
import java.io.File

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.syntax.monad._
import scalaz.syntax.semigroup._
import Iteratee._

trait YggEnumOpsConfig {
  def sortBufferSize: Int
  def sortWorkDir: File
  def sortSerialization: FileSerialization[Vector[SEvent]]
  def flatMapTimeout: Duration
}

trait YggdrasilEnumOpsComponent extends YggConfigComponent with DatasetEnumOpsComponent {
  type YggConfig <: YggEnumOpsConfig

  trait Ops extends DatasetEnumOps {
    val serialization = yggConfig.sortSerialization
    import serialization._

    def flatMap[X, E1, E2, F[_]](d: DatasetEnum[X, E1, F])(f: E1 => DatasetEnum[X, E2, F])(implicit M: Monad[F], asyncContext: ExecutionContext): DatasetEnum[X, E2, F] = 
      DatasetEnum(d.fenum.map { e1e =>
        val result : EnumeratorP[X, Vector[E2], F] = e1e.flatMap { ve => 
          val epv : Vector[EnumeratorP[X, Vector[E2], F]] = ve.map(e => Await.result(f(e).fenum, yggConfig.flatMapTimeout))
          epv.reduce { (ep1 : EnumeratorP[X, Vector[E2], F], ep2: EnumeratorP[X, Vector[E2], F]) => EnumeratorP.enumeratorPMonoid[X, Vector[E2], F].append(ep1, ep2) }
        }
        result
      })

    def sort[X](d: DatasetEnum[X, SEvent, IO], memoAs: Option[(Int, MemoizationContext)])(implicit order: Order[SEvent], asyncContext: ExecutionContext): DatasetEnum[X, SEvent, IO] = {
      import MemoizationContext._
      memoAs.getOrElse((scala.util.Random.nextInt, MemoizationContext.Noop)) match {
        case (memoId, ctx) => ctx[X](memoId) match {
          case Right(memoized) => memoized
          case Left(memoizer)  =>
            DatasetEnum(
              d.fenum map { unsorted =>
                new EnumeratorP[X, Vector[SEvent], IO] {
                  def apply[F[_]](implicit MO: F |>=| IO): EnumeratorT[X, Vector[SEvent], F] = {
                    import MO._
                    implicit val MI = Monad[({type λ[α] = IterateeT[X, Vector[SEvent], F, α]})#λ]

                    val buffer = new Array[SEvent](yggConfig.sortBufferSize)
                    def bufferInsert(i: Int, el: Vector[SEvent]): F[Unit] = MO.promote(IO { el.zipWithIndex foreach { case (e, i2) => buffer(i + i2) = e } }) // TODO: derek says "rethink this whole thing...with chunks" (Daniel agrees).  so, get on that kris
                    def enumBuffer(to: Int) = new EnumeratorP[X, Vector[SEvent], IO] {
                      def apply[F[_]](implicit MO: F |>=| IO): EnumeratorT[X, Vector[SEvent], F] = {
                        import MO._

                        java.util.Arrays.sort(buffer, 0, to, order.toJavaComparator) // TODO: Get this working inside of EnumeratorT.perform again
                        //EnumeratorT.perform[X, Vector[SEvent], F, Unit](MO.promote(IO { java.util.Arrays.sort(buffer, 0, to, order.toJavaComparator) })) |+|
                        enumOne[X, Vector[SEvent], F](Vector(buffer.slice(0, to): _*))
                      }
                    }

                    new EnumeratorT[X, Vector[SEvent], F] {
                      def apply[A] = {
                        val memof = memoizer[F, A](d.descriptor)
                        def sortFile(i: Int) = new File(yggConfig.sortWorkDir, "sort" + memoId + "." + i)

                        def consume(i: Int, chunks: Vector[File], contf: Input[Vector[SEvent]] => IterateeT[X, Vector[SEvent], F, A]): IterateeT[X, Vector[SEvent], F, A] = {
                          if (i < yggConfig.sortBufferSize) cont { (in: Input[Vector[SEvent]]) => 
                            in.fold(
                              el    = el => iterateeT(bufferInsert(i, el) >> consume(i + el.length, chunks, contf).value),
                              empty = consume(i, chunks, contf),
                              eof   = // once we've been sent EOF, we sort the buffer then finally rebuild the iteratee we were 
                                      // originally provided and use that to consume the sorted buffer. We have to pass EOF to
                                      // restore the EOF that we received that triggered the original processing of the stream.
                                      cont(contf) &= 
                                      mergeAll[X, SEvent, IO](chunks.map(reader[X]) :+ enumBuffer(i): _*).apply[F] &= 
                                      EnumeratorT.perform[X, Vector[SEvent], F, Unit](MO.promote(chunks.foldLeft(IO()) { (io, f) => io >> IO(f.delete) })) &= 
                                      enumEofT
                            )
                          } else {
                            MI.bind(writer(sortFile(chunks.size)) &= enumBuffer(i).apply[F]) { file => 
                              consume(0, chunks :+ file, contf)
                            }
                          }
                        }

                        (s: StepT[X, Vector[SEvent], F, A]) => memof(s mapCont { contf => consume(0, Vector.empty[File], contf) &= unsorted[F] })
                      }
                    }
                  }
                }
              }
            )
        }
      }
    }
    
    def memoize[X](d: DatasetEnum[X, SEvent, IO], memoId: Int, memoizationContext: MemoizationContext)(implicit asyncContext: ExecutionContext): DatasetEnum[X, SEvent, IO] = {
      memoizationContext[X](memoId) match {
        case Right(enum) => enum
        case Left(memoizer) =>
          DatasetEnum(
            d.fenum map { unmemoized =>
              new EnumeratorP[X, Vector[SEvent], IO] {
                def apply[F[_]](implicit MO: F |>=| IO): EnumeratorT[X, Vector[SEvent], F] = {
                  import MO._
                  import MO.MG.bindSyntax._

                  new EnumeratorT[X, Vector[SEvent], F] {
                    def apply[A] = {
                      val memof = memoizer[F, A](d.descriptor)
                      (s: StepT[X, Vector[SEvent], F, A]) => memof(s.pointI) &= unmemoized[F]
                    }
                  }
                }
              }
            }
          )
      }
    }

    def group[X](d: DatasetEnum[X, SEvent, IO], fs: FileSerialization[Vector[(Key, SEvent)]])(f: SEvent => Key)(implicit ord: Order[Key]): Future[EnumeratorP[X, (Key, DatasetEnum[X, SEvent, IO]), IO]] = {
      import MemoizationContext._
      type Group = (Key, DatasetEnum[X, SEvent, IO])
      
      implicit val pairOrder = ord.contramap((_: (Key, SEvent))._1)

      def chunked(enum: EnumeratorP[X, Vector[(Key, SEvent)], IO]): EnumeratorP[X, Group, IO] = {
        new EnumeratorP[X, Group, IO] {
          def apply[G[_]](implicit MO: G |>=| IO) = new EnumeratorT[X, Group, G] {
            import MO._

            def apply[A] = {
              type LoopEnum = StepT[X, Vector[SEvent], G, A] => IterateeT[X, Vector[SEvent], G, A]
              def nextStep(chunk: Vector[SEvent]) = (i: IterateeT[X, Vector[SEvent], G, A]) => iterateeT(i.value flatMap { s => s.mapCont(contf => contf(elInput(chunk))).value })

              def loop(last: Option[Key], buffer: Vector[SEvent], step: LoopEnum): 
              Input[Vector[(Key, SEvent)]] => IterateeT[X, Vector[(Key, SEvent)], G, Option[(Key, LoopEnum)]] = 
                (_: Input[Vector[(Key, SEvent)]]).fold(
                  el = el => 
                    el.headOption match {
                      case Some((k, _)) if last.forall(_ == k) => 
                        val (prefix, remainder) = el.span(_._1 == k)
                        val merged = buffer ++ prefix.map(_._2)

                        if (remainder.isEmpty) {
                          if (merged.size < yggConfig.sortBufferSize) {
                            // we don't know whether the next chunk will have more data corresponding to this key, so
                            // we just continue, appending to our buffer but not advancing the step.
                            cont(loop(last, merged, step))
                          } else {
                            // we need to advance the step with a chunk of maximal size, then
                            // continue with the rest in the buffer.
                            val (chunk, rest) = merged.splitAt(yggConfig.sortBufferSize)
                            cont(loop(last, rest, step andThen nextStep(chunk)))
                          }
                        } else {
                          if (merged.size < yggConfig.sortBufferSize) {
                            // we need to advance the step just once, then be done.
                            done[X, Vector[(Key, SEvent)], G, Option[(Key, LoopEnum)]](Some((k, step andThen nextStep(merged))), elInput(remainder))
                          } else {
                            //advance the step with a chunk of maximal size, then the rest, then be done.
                            val (chunk, rest) = merged.splitAt(yggConfig.sortBufferSize)
                            done[X, Vector[(Key, SEvent)], G, Option[(Key, LoopEnum)]](Some((k, step andThen nextStep(chunk) andThen nextStep(rest))), elInput(remainder))
                          }
                        }

                      case Some(_) =>
                        done[X, Vector[(Key, SEvent)], G, Option[(Key, LoopEnum)]](last.map(k => (k, step)), elInput(el))
                        
                      case None =>
                        cont(loop(last, buffer, step))
                    },
                  empty = cont(loop(last, buffer, step)),
                  eof = done[X, Vector[(Key, SEvent)], G, Option[(Key, LoopEnum)]](last.map(k => (k, step andThen nextStep(buffer))), eofInput)
                )

              (s: StepT[X, Group, G, A]) => {
                sys.error("todo")
              }
            }
          }
        }
      }

      d.fenum map { enum => 
        new EnumeratorP[X, Group, IO] {
          def apply[G[_]](implicit MO: G |>=| IO) = new EnumeratorT[X, Group, G] {
            import MO._

            def apply[A] = {
              val memoId = 0 //todo, fix this
              def sortFile(i: Int) = new File(yggConfig.sortWorkDir, "groupsort" + memoId + "." + i)

              val buffer = new Array[(Key, SEvent)](yggConfig.sortBufferSize)
              def bufferInsert(i: Int, el: Vector[(Key, SEvent)]): G[Unit] = MO.promote(IO { el.zipWithIndex foreach { case (e, i2) => buffer(i + i2) = e } }) 
              def enumBuffer(to: Int) = new EnumeratorP[X, Vector[(Key, SEvent)], IO] {
                def apply[F[_]](implicit MO: F |>=| IO): EnumeratorT[X, Vector[(Key, SEvent)], F] = {
                  import MO._

                  java.util.Arrays.sort(buffer, 0, to, pairOrder.toJavaComparator) // TODO: Get this working inside of EnumeratorT.perform again
                  //EnumeratorT.perform[X, Vector[SEvent], F, Unit](MO.promote(IO { java.util.Arrays.sort(buffer, 0, to, order.toJavaComparator) })) |+|
                  enumOne[X, Vector[(Key, SEvent)], F](Vector(buffer.slice(0, to): _*))
                }
              }

              def consume(i: Int, chunks: Vector[File]): IterateeT[X, Vector[SEvent], G, (Int, Vector[File])] = {
                if (i < yggConfig.sortBufferSize) cont { (in: Input[Vector[SEvent]]) => 
                  in.fold(
                    el    = el => iterateeT(bufferInsert(i, el map { ev => (f(ev), ev) }) >> consume(i + el.length, chunks).value),
                    empty = consume(i, chunks),
                    eof   = done((i, chunks), eofInput)
                  )
                } else {
                  fs.writer(sortFile(chunks.size)).withResult(enumBuffer(i).apply[G]) { file => 
                    consume(0, chunks :+ file)
                  }
                }
              }

              (s: StepT[X, Group, G, A]) => consume(0, Vector.empty[File]).withResult(enum[G]) {
                case (i, files) => chunked(mergeAll(files.map(fs.reader[X]) :+ enumBuffer(i): _*)).apply[G].apply(s)
              }
            }
          }
        }
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
