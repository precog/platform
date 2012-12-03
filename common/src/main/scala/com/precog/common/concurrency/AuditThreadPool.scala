package com.precog.common
package concurrency

import java.lang.management.ManagementFactory
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.locks.ReentrantLock

import scala.annotation.tailrec

class AuditExecutor(val name: String, minThreads: Int, maxThreads: Int, maxQueueDepth: Option[Int], idleTimeout: Long) extends Executor {
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

  private[this] val workQueue : BlockingQueue[Runnable] = maxQueueDepth.map { depth =>
    new ArrayBlockingQueue[Runnable](depth)
  }.getOrElse (new LinkedBlockingQueue[Runnable]())

  def size = threadCount.get
  def activeSize = activeThreadCount.get

  private[this] val threadCount = new AtomicInteger(minThreads)
  private[this] val activeThreadCount = new AtomicInteger(0)
  private[this] val cumulativeCpuTime = new AtomicLong(0)

  private[this] final val threadUpdateLock = new AnyRef()

  private[this] var workers = (1 to minThreads).map { i => new WorkerThread(i) }.toSet
  workers.foreach(_.start())

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

  private val threadMXBean = ManagementFactory.getThreadMXBean

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
