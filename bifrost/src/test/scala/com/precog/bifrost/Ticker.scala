/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.bifrost

import java.util.concurrent.atomic.AtomicLong

import com.precog.common._

import akka.actor._
import akka.dispatch._
import akka.util.Duration

import blueeyes.util.Clock

import org.joda.time._

import scalaz._
import scalaz.std.option._
import scalaz.syntax.monad._

/** Message used by `Ticker` to process 1 tick. */
case object Tick

/** Message to schedule a thunk for execution in a `Ticker`. */
case class Schedule(at: Long, thunk: () => Any)

/**
 * A ticker is an attempt at being able to get a bit of sanity back into timing
 * futures. Basically, it let's you schedule a thunk to be run at a specific
 * point in time (a particular tick). The ticker can be advanced by sending it
 * a `Tick` message. We can also schedule thunks by sending a `Schedule`
 * message. The schedule is always relative to the time at which the message is
 * processed, so all scheduled thunks will always run.
 *
 * This actor shouldn't be used directly, but in conjunction with the
 * `SchedulableFuturesModule`, which provides a schedule method to create a
 * `Future` out of a thunk. The futures are still no deterministic, but the
 * combination of these 2 classes allows the timing of future completion to be
 * synchronized a bit more.
 */
class Ticker(ticks: AtomicLong) extends Actor {
  private var schedule: Map[Long, List[() => Any]] = Map.empty

  def receive = {
    case Tick =>
      val t = ticks.get()
      schedule get t map { thunks =>
        schedule = schedule - t
        thunks.reverse foreach { t =>
          t()
        }
      } getOrElse {
        ticks.getAndIncrement()
      }

    case Schedule(n, thunk) =>
      val t = ticks.get() + n
      schedule = schedule + (t -> (thunk :: schedule.getOrElse(t, Nil)))
  }
}

/**
 * This is a `Clock` whose time comes from an atomic long, which can be updated
 * manually. The current time will always be `start + duration * ticks.get`,
 * where the duration is in milliseconds.
 */
class ManualClock(ticks: AtomicLong, start: DateTime = new DateTime, val duration: Long = 50) extends Clock {
  def now(): DateTime = {
    val cur = start.getMillis() + ticks.get() * duration
    new DateTime(cur)
  }

  def instant(): Instant = new Instant(now().getMillis())

  def nanoTime(): Long = now().getMillis() * 1000
}

/**
 * This trait provides a way to schedule thunks for execution at somewhat
 * predictable times. The `schedule` method can be given the number of ticks
 * to wait before executing a thunk and will return a Future which will
 * complete at that time. It also provides a clock whose time is directly tied
 * to `ticks`, which would presumably be used by the `Ticker` to keep track of
 * ticks processed.
 */
trait SchedulableFuturesModule {
  implicit def executionContext: ExecutionContext

  val ticks: AtomicLong = new AtomicLong

  val clock: ManualClock = new ManualClock(ticks)

  def ticker: ActorRef

  def await[A, B](f: Future[A], atMost: Duration)(g: A => B): Future[B] = {
    schedule(0) {
      g(Await.result(f, atMost))
    }
  }

  def schedule[A](n: Long)(f: => A): Future[A] = {
    val promise = Promise[A]()
    val thunk: () => Any = { () =>
      try {
        promise.success(f)
      } catch {
        case ex => promise.failure(ex)
      }
    }

    ticker ! Schedule(n, thunk)

    promise.future
  }

  def waitFor(n: Long): Future[Unit] = schedule(n) { () }
}
