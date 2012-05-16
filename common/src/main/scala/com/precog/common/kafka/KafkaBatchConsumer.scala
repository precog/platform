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
package common 
package kafka

import scala.annotation.tailrec

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import com.weiglewilczek.slf4s._ 

import _root_.kafka.api._
import _root_.kafka.consumer._
import _root_.kafka.message._

trait BatchConsumer {
  def ingestBatch(offset: Long, bufferSize: Int): Iterable[MessageAndOffset]
  def close(): Unit
}

object BatchConsumer {
  val NullBatchConsumer = new BatchConsumer {
    def ingestBatch(offset: Long, bufferSize: Int) = List()
    def close() = ()
  }
}

class KafkaBatchConsumer(host: String, port: Int, topic: String) extends BatchConsumer {
 
  private val timeout = 5000
  private val buffer = 64 * 1024

  private lazy val consumer = new SimpleConsumer(host, port, timeout, buffer) 

  def ingestBatch(offset: Long, bufferSize: Int): MessageSet = {
    val fetchRequest = new FetchRequest(topic, 0, offset, bufferSize)

    consumer.fetch(fetchRequest)
  }

  def close() {
    consumer.close
  }
}
