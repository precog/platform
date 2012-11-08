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
          val builder = if (value == null) {        // TODO gross!
            cbf()
          } else {
            val back = cbf(value)
            back ++= value
            back
          }
          
          builder += b
          
          value = builder.result()
          
          if (setterThread != null) {
            isSet = true
          }
        }
      } finally {
        lock.unlock()
      }
    }
  }
  
  def ++=[E](c: A)(implicit unpack: Unpack[A, E], cbf: CanBuildFrom[A, E, A], evidence2: A <:< TraversableOnce[E]) { 
    if (!isForced || setterThread != null) {
      lock.lock()
      try {
        if (!isForced || setterThread != null) {
          val builder = if (value == null) {        // TODO gross!
            cbf()
          } else {
            val back = cbf(value)
            back ++= value
            back
          }
          
          builder ++= evidence2(c)
          
          value = builder.result()
          
          if (setterThread != null) {
            isSet = true
          }
        }
      } finally {
        lock.unlock()
      }
    }
  }
  
  def appendFrom[E, Coll[_]](a: Atom[Coll[E]])(implicit cbf: CanBuildFrom[Coll[E], E, A], evidence: A =:= Coll[E], evidence2: Coll[E] <:< TraversableOnce[E]) {
    if (!isForced || setterThread != null) {
      lock.lock()
      try {
        if (!isForced || setterThread != null) {
          val builder = if (value == null) {        // TODO gross!
            cbf()
          } else {
            val current = evidence(value)
            val back = cbf(current)
            back ++= current
            back
          }
         
          // not thread safe, basically horrible
          if (a.value != null) {
            builder ++= evidence2(a.value)
          }
          
          value = builder.result()
          
          if (setterThread != null) {
            isSet = true
          }
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
            setterThread = null
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

class Unpack[C, E]

object Unpack {
  implicit def unpack1[CC[_], T] = new Unpack[CC[T], T]
  implicit def unpack2[CC[_, _], T, U] = new Unpack[CC[T, U], (T, U)]
}
