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
package com.precog.common
package concurrency

import java.lang.management.ManagementFactory
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.locks.ReentrantLock

import scala.annotation.tailrec

class AuditExecutor(val name: String, minThreads: Int, maxThreads: Int, maxQueueDepth: Option[Int], idleTimeout: Long) extends Executor {
  private[this] val workQueue : BlockingQueue[Runnable] = maxQueueDepth.map { depth =>
    new ArrayBlockingQueue[Runnable](depth)
  }.getOrElse (new LinkedBlockingQueue[Runnable]())

  private[this] val threadCount = new AtomicInteger(minThreads)
  private[this] val activeThreadCount = new AtomicInteger(0)
  private[this] val cumulativeCpuTime = new AtomicLong(0)

  private[this] final val threadUpdateLock = new AnyRef()

  private[this] var workers = (1 to minThreads).map { i => new WorkerThread(i) }.toSet
  workers.foreach(_.start())

  private val threadMXBean = ManagementFactory.getThreadMXBean

  def size = threadCount.get
  def activeSize = activeThreadCount.get

  def execute(task: Runnable): Unit = {
    if (! workQueue.offer(task)) {
      throw new RejectedExecutionException()
    }

    // We may be able to allocate a new WorkerThread if all threads are currently active and we have room to grow
    if (activeThreadCount.get == threadCount.get) {
      threadUpdateLock.synchronized {
        if (threadCount.get < maxThreads) {
          val worker = new WorkerThread(threadCount.incrementAndGet)
          workers += worker
          worker.start()
        }
      }
    }
  }

  def cpuDelta: Long = {
    val currentDelta = workers.map(_.cpuDelta).filterNot(_ < 0).sum
    cumulativeCpuTime.addAndGet(currentDelta)
    currentDelta
  }

  def totalTime: Long = cumulativeCpuTime.get

  private def workerFinished(worker: WorkerThread): Unit = {
    threadUpdateLock.synchronized {
      workers -= worker
      cumulativeCpuTime.addAndGet(worker.cpuDelta)
      threadCount.decrementAndGet
    }
  }

  /**
    Called by threads to determine if they should continue waiting when the queue has no work for them
    */
  private def shouldContinue: Boolean = {
    val currentCount = threadCount.get
    if (currentCount > minThreads) {
      threadUpdateLock.synchronized {
        try {
          // If this is the first thread to volunteer for reaping, let it go
          if (threadCount.compareAndSet(currentCount, currentCount - 1)) {
            false
          } else {
            true
          }
        }
      }
    } else {
      true
    }
  }

  private class WorkerThread(id: Int) extends Thread(name + "-" + id) {
    private[this] var lastCpuTime = 0l

    def cpuDelta = synchronized {
      val newTime = threadMXBean.getThreadCpuTime(this.getId)
      val delta = newTime - lastCpuTime
      lastCpuTime = newTime
      delta
    }

    override def run(): Unit = try {
      @tailrec
      def processQueue: Unit = {
        val nextJob = workQueue.poll(idleTimeout, TimeUnit.MILLISECONDS)
        if (nextJob != null) {
          activeThreadCount.incrementAndGet()
          try {
            nextJob.run()
          } finally {
            activeThreadCount.decrementAndGet()
          }
          processQueue
        } else if (shouldContinue) {
          processQueue
        }
      }

      processQueue
    } finally {
      workerFinished(this)
    }
  }
}
