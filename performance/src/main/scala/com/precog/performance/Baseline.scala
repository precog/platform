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
