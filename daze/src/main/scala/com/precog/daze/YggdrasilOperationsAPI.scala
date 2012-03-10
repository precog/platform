package com.precog.daze

import com.precog.yggdrasil._
import com.precog.yggdrasil.util._
import com.precog.util._

import akka.dispatch.Await
import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.util.Duration
import java.io.File
import java.util.concurrent.TimeoutException

import scala.annotation.tailrec
import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.syntax.monad._
import scalaz.syntax.foldable._
import scalaz.syntax.semigroup._
import Iteratee._

trait YggEnumOpsConfig {
  def sortBufferSize: Int
  def sortWorkDir: File
  def flatMapTimeout: Duration
}

trait YggdrasilEnumOpsComponent extends YggConfigComponent with DatasetEnumOpsComponent {
  type YggConfig <: YggEnumOpsConfig

  trait Ops extends DatasetEnumOps {
    def flatMap[E1, E2, F[_]](d: DatasetEnum[X, E1, F])(f: E1 => DatasetEnum[X, E2, F])(implicit M: Monad[F], asyncContext: ExecutionContext): DatasetEnum[X, E2, F] = 
      DatasetEnum(d.fenum.map { e1 =>
        e1 flatMap { 
          (_: Vector[E1]).foldLeft(EnumeratorP.empty[X, Vector[E2], F]) { 
            case (acc, e) => 
              try {
                acc |+| Await.result(f(e).fenum, yggConfig.flatMapTimeout)
              } catch {
                case ex => EnumeratorP.pointErr(new RuntimeException("Timed out in flatMap; returning error enumerator. ", ex))
              }
          }
        }
      })

    def sort[E <: AnyRef](d: DatasetEnum[X, E, IO], memoAs: Option[(Int, MemoizationContext)])(implicit order: Order[E], cm: Manifest[E], fs: FileSerialization[Vector[E]], asyncContext: ExecutionContext): DatasetEnum[X, E, IO] = {
      DatasetEnum(
        d.fenum map { unsorted =>
          val (memoId, memoctx) = memoAs.getOrElse((scala.util.Random.nextInt, MemoizationContext.Noop)) 
          new EnumeratorP[X, Vector[E], IO] {
            def apply[G[_]](implicit MO: G |>=| IO): EnumeratorT[X, Vector[E], G] = {
              import MO._

              new EnumeratorT[X, Vector[E], G] {
                def apply[A] = (s: StepT[X, Vector[E], G, A]) => 
                  memoctx.memoizing[X, Vector[E]](memoId) match {
                    case Right(memoized) => s.pointI &= memoized[G]
                    case Left(memoizer)  => memoizer(s.pointI &= sorted("sort" + memoId, unsorted, identity[E]).apply[G])
                  }
              }
            }
          }
        }
      )
    }
    
    def memoize[E](d: DatasetEnum[X, E, IO], memoId: Int, memoctx: MemoizationContext)
                     (implicit fs: FileSerialization[Vector[E]], asyncContext: ExecutionContext): DatasetEnum[X, E, IO] = 
      DatasetEnum(
        d.fenum map { unmemoized =>
          new EnumeratorP[X, Vector[E], IO] {
            def apply[G[_]](implicit MO: G |>=| IO): EnumeratorT[X, Vector[E], G] = {
              import MO._
              import MO.MG.bindSyntax._

              new EnumeratorT[X, Vector[E], G] {
                def apply[A] = (s: StepT[X, Vector[E], G, A]) => 
                  memoctx.memoizing[X, Vector[E]](memoId) match {
                    case Right(memoized) => s.pointI &= memoized[G]
                    case Left(memoizer)  => memoizer(s.pointI) &= unmemoized[G]
                  }
              }
            }
          }
        }
      )

    def group(d: DatasetEnum[X, SEvent, IO], memoId: Int, bufctx: BufferingContext)(keyFor: SEvent => Key)
             (implicit ord: Order[Key], fs: FileSerialization[Vector[SEvent]], kvs: FileSerialization[Vector[(Key, SEvent)]], asyncContext: ExecutionContext): 
             Future[EnumeratorP[X, (Key, DatasetEnum[X, SEvent, IO]), IO]] = {
      type Group = (Key, DatasetEnum[X, SEvent, IO])
      
      implicit val pairOrder: Order[(Key, SEvent)] = ord.contramap((_: (Key, SEvent))._1)

      def chunked[G[_]](implicit MO: G |>=| IO): EnumerateeT[X, Vector[(Key, SEvent)], Group, G] = new EnumerateeT[X, Vector[(Key, SEvent)], Group, G] {
        import MO._

        def apply[A]: StepT[X, Group, G, A] => IterateeT[X, Vector[(Key, SEvent)], G, StepT[X, Group, G, A]] = step => {
          val i = 0
          step.fold(
            cont = (contf: Input[Group] => IterateeT[X, Group, G, A]) =>
              headDoneOr[X, Vector[(Key, SEvent)], G, StepT[X, Group, G, A]](
                scont(contf),
                v => v.headOption match {
                  case Some((key, _)) => 
                    iterateeT(bufctx.buffering[X, Vector[SEvent], G](i).value.flatMap(s => loop(key, Vector(), s)(elInput(v)).value)).flatMap(g => contf(elInput(g)) >>== apply[A])

                  case None => 
                    contf(emptyInput) >>== apply[A]
                }
              ),
            done = (a, r) => done(sdone(a, r), emptyInput),
            err  = x => err(x)
          )
        }

        def loop(last: Key, buffer: Vector[SEvent], bufStep: StepT[X, Vector[SEvent], G, EnumeratorP[X, Vector[SEvent], IO]]): Input[Vector[(Key, SEvent)]] => IterateeT[X, Vector[(Key, SEvent)], G, Group] = {
          def loopDone(bufIter: IterateeT[X, Vector[SEvent], G, EnumeratorP[X, Vector[SEvent], IO]], remainder: Input[Vector[(Key, SEvent)]]) = 
            iterateeT(
              (bufIter &= enumEofT).foldT(
                cont = contf  => sys.error("diverging iteratee"),
                done = (a, _) => MO.MG.point(sdone[X, Vector[(Key, SEvent)], G, Group]((last, DatasetEnum(Future(a))), remainder)),
                err  = x      => MO.MG.point(serr[X, Vector[(Key, SEvent)], G, Group](x))
              )
            )
            
          (in: Input[Vector[(Key, SEvent)]]) => {
            in.fold(
              el = el => 
                el.headOption match {
                  case Some((`last`, _)) =>
                    val (prefix, remainder) = el.span(_._1 == last)
                    val merged = buffer ++ prefix.map(_._2)

                    if (remainder.isEmpty) {
                      if (merged.size < yggConfig.sortBufferSize) {
                        // we don't know whether the next chunk will have more data corresponding to this key, so
                        // we just continue, appending to our buffer but not advancing the bufstep.
                        cont(loop(last, merged, bufStep))
                      } else {
                        // we need to advance the bufstep with a chunk of maximal size, then
                        // continue with the rest in the buffer.
                        val (chunk, rest) = merged.splitAt(yggConfig.sortBufferSize)
                        iterateeT(
                          for {
                            bufStep  <- bufStep.mapCont(_(elInput(chunk))).value
                            loopStep <- cont(loop(last, rest, bufStep)).value
                          } yield loopStep
                        )
                      }
                    } else {
                      if (merged.size < yggConfig.sortBufferSize) {
                        // we need to advance the bufstep just once, then be done.
                        loopDone(bufStep.mapCont(_(elInput(merged))), elInput(remainder))
                      } else {
                        //advance the bufstep with a chunk of maximal size, then the rest, then be done.
                        val (chunk, rest) = merged.splitAt(yggConfig.sortBufferSize)
                        loopDone((bufStep.mapCont(_(elInput(chunk))) >>== (s => s.mapCont(_(elInput(rest))))), elInput(remainder))
                      }
                    }

                  case Some(_) => 
                    loopDone(bufStep.pointI, in)

                  case None =>
                    cont(loop(last, buffer, bufStep))
                },
              empty = cont(loop(last, buffer, bufStep)),
              eof   = loopDone(bufStep.mapCont(_(elInput(buffer))), eofInput)
            )
          }
        }
      }

      d.fenum map { enum => 
        new EnumeratorP[X, Group, IO] {
          def apply[G[_]](implicit MO: G |>=| IO) = new EnumeratorT[X, Group, G] {
            import MO._

            def apply[A] = {
              (_: StepT[X, Group, G, A]).pointI &= chunked[G].run(sorted("groupsort" + memoId, enum, (ev: SEvent) => (keyFor(ev), ev)).apply[G])
            }
          }
        }
      }
    }

    private def sorted[X, E, B <: AnyRef](filePrefix: String, enum: EnumeratorP[X, Vector[E], IO], f: E => B)
                                         (implicit ord: Order[B], fs: FileSerialization[Vector[B]], mf: Manifest[B]): EnumeratorP[X, Vector[B], IO] = 
      new EnumeratorP[X, Vector[B], IO] {
        def apply[G[_]](implicit MO: G |>=| IO) = new EnumeratorT[X, Vector[B], G] {
          import MO._

          def apply[A] = {
            def sortFile(i: Int) = new File(yggConfig.sortWorkDir, filePrefix + "." + i)

            val buffer = new Array[B](yggConfig.sortBufferSize)

            def bufferInsert(i: Int, v: Vector[E]): IO[Int] = {
              @tailrec def insert(j: Int, iter: Iterator[E]): Int = {
                if (i+j < buffer.length && iter.hasNext) {
                  buffer(i+j) = f(iter.next())
                  insert(j+1, iter)
                } else j
              }

              IO { insert(0, v.iterator) } 
            }

            def enumBuffer(to: Int): EnumeratorP[X, Vector[B], IO] = new EnumeratorP[X, Vector[B], IO] {
              def apply[G[_]](implicit MO: G |>=| IO) = new EnumeratorT[X, Vector[B], G] {
                import MO._

                def apply[A] = {
                  (_: StepT[X, Vector[B], G, A]).pointI &= {
                    //EnumeratorT.perform[X, Vector[B], G, Unit](MO.promote(IO { java.util.Arrays.sort(buffer, 0, to, ord.toJavaComparator) })) &=
                    java.util.Arrays.sort(buffer, 0, to, ord.toJavaComparator)
                    enumOne[X, Vector[B], G](Vector(buffer.slice(0, to): _*))
                  }
                }
              }
            }

            type ChunkConsumer = IterateeT[X, Vector[E], IO, (Int, Vector[File])]
            def spill(i: Int, sortFragments: Vector[File])(nextStep: File => ChunkConsumer): ChunkConsumer = {
              fs.writer[X, IO](sortFile(sortFragments.size)).withResult(enumBuffer(i).apply[IO]) { nextStep }
            }

            def consumeChunk(i: Int, sortFragments: Vector[File], chunk: Vector[E]): ChunkConsumer = {
              if (chunk.isEmpty) consume(i, sortFragments)
              else iterateeT(
                for {
                  iadv     <- bufferInsert(i, chunk)
                  nextStep <- if (iadv < chunk.length) spill(i + iadv, sortFragments) { file => consumeChunk(0, sortFragments :+ file, chunk.drop(iadv)) } value
                              else                     consume(i + chunk.length, sortFragments).value
                } yield nextStep
              )
            }

            def consume(i: Int, sortFragments: Vector[File]): ChunkConsumer = cont { 
              (_: Input[Vector[E]]).fold(
                el    = el => consumeChunk(i, sortFragments, el),
                empty = consume(i, sortFragments),
                eof   = done((i, sortFragments), eofInput)
              )
            }

            (s: StepT[X, Vector[B], G, A]) => consume(0, Vector.empty[File]).mapI(MO).withResult(enum[G]) {
              case (i, files) => 
                //todo: need to restore correct chunk sizes
                val sortFragmentEnums: Seq[EnumeratorP[X, Vector[B], IO]] = files.map(fs.reader[X]) :+ enumBuffer(i)
                val sorted = mergeAllChunked(sortFragmentEnums: _*) |+|
                             EnumeratorP.perform[X, Vector[B], IO, Unit](files.foldLeft(IO()) { (io, f) => io >> IO(f.delete) })

                s.pointI &= sorted[G]
            }
          }
        }
      }
  }
}

// vim: set ts=4 sw=4 et:
