package com.precog
package util

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock

import scala.collection.generic.CanBuildFrom

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
  
  def +=[B](b: B)(implicit cbf: CanBuildFrom[A, B, A], evidence: A <:< TraversableOnce[B]) {
    if (!isForced || setterThread != null) {
      lock.lock()
      try {
        if (!isForced || setterThread != null) {
          val builder = if (!isSet) {
            cbf()
          } else {
            val back = cbf(value)
            back ++= value
            back
          }
          
          builder += b
          
          value = builder.result()
          isSet = true
        }
      } finally {
        lock.unlock()
      }
    }
  }
  
  def ++=[E, Coll[_]](c: Coll[E])(implicit cbf: CanBuildFrom[Coll[E], E, A], evidence: A =:= Coll[E], evidence2: Coll[E] <:< TraversableOnce[E]) {
    if (!isForced || setterThread != null) {
      lock.lock()
      try {
        if (!isForced || setterThread != null) {
          val builder = if (!isSet) {
            cbf()
          } else {
            val current = evidence(value)
            val back = cbf(current)
            back ++= current
            back
          }
          
          builder ++= evidence2(c)
          
          value = builder.result()
          isSet = true
        }
      } finally {
        lock.unlock()
      }
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
            
            if (!isSet) {
              sys.error("Unable to self-populate atom (value not set following attempted population)")
            }
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
    }
  }
  
  def atom[A]: Atom[A] = new Atom[A]
}
