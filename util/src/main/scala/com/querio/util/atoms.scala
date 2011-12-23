package com.querio
package util

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock

trait Source[+A] {
  def apply(): A
  def into(sink: Sink[A])
}

trait Sink[-A] {
  def update(a: A)
  def from(src: Source[A])
}

class Atom[A] extends Source[A] with Sink[A] {
  
  @volatile
  private var value: A = null.asInstanceOf[A]
  
  @volatile
  private var isSet = false
  
  @volatile
  private var isForced = false
  
  @volatile
  private var setterThread: Thread = null
  
  @volatile
  private var targets = Set[Sink[A]]()
  
  private val lock = new ReentrantLock
  private val semaphore = new AnyRef
  
  protected def populate() {
    sys.error("Cannot self-populate atom")
  }
  
  def update(a: A) {
    lock.lock()
    try {
      if (!isSet || !isForced) {
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
  
  def from(source: Source[A]) {
    source.into(this)
  }
  
  def into(sink: Sink[A]) {
    if (isSet) {
      sink() = value
    } else {
      lock.lock()
      if (isSet) {
        sink() = value
      } else {
        try {
          targets += sink
        } finally {
          lock.unlock()
        }
      }
    }
  }
  
  // TODO should force source atom (if any) at this point
  def apply(): A = {
    isForced = true
    
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
    
    if (!targets.isEmpty) {
      lock.lock()
      try {
        targets foreach { _() = value }
        targets = Set()
      } finally {
        lock.unlock()
      }
    }
    
    value
  }
}

object Atom {
  def atom[A](f: =>Unit): Atom[A] = new Atom[A] {
    override def populate() = {
      f
      
      if (!isSet) {
        sys.error("Unable to self-populate atom (value not set following attempted population)")
      }
    }
  }
  
  def atom[A]: Atom[A] = new Atom[A]
}


trait Aggregate[A, Coll[_]] extends Source[Coll[A]] {
  private val ref = new AtomicReference(init)
  
  @volatile
  private var isForced = false
  
  @volatile
  private var targets = Set[Sink[Coll[A]]]()
  
  private val lock = new ReentrantLock
  
  def +=(a: A)
  def ++=(s: Coll[A])
  
  def apply() = {
    if (isForced) {
      ref.get
    } else {
      lock.lock()
      try {
        if (isForced) {
          ref.get
        } else {
          isForced = true
          val back = ref.get
          
          targets foreach { _() = back }
          targets = Set()
          
          back
        }
      } finally {
        lock.unlock()
      }
    }
  }
  
  def into(sink: Sink[Coll[A]]) {
    if (isForced) {
      sink() = ref.get
    } else {
      lock.lock()
      try {
        if (isForced) {
          sink() = ref.get
        } else {
          targets += sink
        }
      } finally {
        lock.unlock()
      }
    }
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
