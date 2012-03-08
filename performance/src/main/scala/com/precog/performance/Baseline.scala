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
package com.precog.performance

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props

object Baseline extends App {
  var map = Map[Long, Long]()

  var cnt = 0L
  val limit = 1000000L

  while(cnt < limit) {
    map += (cnt -> (limit - cnt))
    cnt += 1
  }

  var start = System.nanoTime

  cnt = 0L
  var sum = 0L

  while(cnt < limit) {
    sum += map(cnt)
    cnt += 1
  }

  val time = System.nanoTime - start

  println("While: %.02f".format(time/ 1000000.0))


  val system = ActorSystem("test")
  val actor1 = system.actorOf(Props(new Actor1), "actor1")
  val actor2 = system.actorOf(Props(new Actor2(actor1, limit)), "actor2")
  
  cnt = 0

  while(cnt < limit) {
    actor2 ! Serve
    cnt += 1
  }

}

case object Serve
case class Ping(replyTo: ActorRef)
case object Pong 

class Actor1 extends Actor {
  def receive = {
    case Ping(replyTo) =>
      replyTo ! Pong 
  }
}

class Actor2(actor: ActorRef, expected: Long) extends Actor {
  val start = System.nanoTime 
  var cnt = 1L
  def receive = {
    case Serve => 
      actor ! Ping(self)
    case Pong =>
      cnt += 1
      if(cnt >= expected) {
        println("Time: %.02f".format((System.nanoTime - start) / 1000000.0))
      }
  }
}
