package com.reportgrid.quirrel
package util

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock

trait Source[+A] {
  def apply(): A
}

trait Sink[-A] {
  def update(a: A)
}

class Atom[A] extends Source[A] with Sink[A] {
  
  @volatile
  private var value: A = null.asInstanceOf[A]
  
  @volatile
  private var isSet = false
  
  @volatile
  private var setterThread: Thread = null
  
  private val lock = new ReentrantLock
  private val semaphore = new AnyRef
  
  protected def populate() {
    sys.error("Cannot self-populate atom")
  }
  
  def update(a: A) {
    lock.lock()
    try {
      if (!isSet) {
        value = a
        isSet = true
        setterThread = null
        
        semaphore synchronized {
          semaphore.notifyAll()
        }
      }
    } finally {
      lock.unlock()
    }
  }
  
  def apply(): A = {
    if (isSet) {
      value
    } else {
      lock.lock()
      try {
        if (isSet) {
          value
        } else if (setterThread != null) {
          if (setterThread == Thread.currentThread) {
            sys.error("Recursive atom definition detected")
          } else {
            lock.unlock()
            try {
              semaphore synchronized {
                semaphore.wait()
              }
            } finally {
              lock.lock()
            }
            value
          }
        } else {
          setterThread = Thread.currentThread
          lock.unlock()
          try {
            populate()
          } finally {
            lock.lock()
          }
          value
        }
      } finally {
        lock.unlock()
      }
    }
    value
  }
}

object Atom {
  def atom[A](f: =>Unit): Atom[A] = new Atom[A] {
    override def populate() = f
  }
  
  def atom[A]: Atom[A] = new Atom[A]
}


trait Aggregate[A, Coll[_]] extends Source[Coll[A]] {
  private val ref = new AtomicReference(init)
  
  @volatile
  private var isForced = false
  
  def +=(a: A)
  def ++=(s: Coll[A])
  
  def apply() = {
    isForced = true
    ref.get
  }
  
  protected final def swap(f: Coll[A] => Coll[A]) {
    var successful = false
    while (!isForced && !successful) {
      val set = ref.get
      val set2 = f(set)
      successful = ref.compareAndSet(set, set2)
    }
  }
  
  protected def init: Coll[A]
}

class SetAtom[A] extends Aggregate[A, Set] {
  def +=(a: A) = swap { _ + a }
  def ++=(s: Set[A]) = swap { _ ++ s }
  
  protected def init = Set[A]()
}

class MapAtom[A, B] extends Aggregate[(A, B), ({ type L[_] = Map[A, B] })#L] {
  def +=(pair: (A, B)) = swap { _ + pair }
  def ++=(m: Map[A, B]) = swap { _ ++ m }
  
  protected def init = Map[A, B]()
}
