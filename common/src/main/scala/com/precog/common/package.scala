package com.precog

import blueeyes.json.JsonAST._

import akka.actor.ActorSystem
import akka.dispatch.{Future, ExecutionContext}

package object common {
  
  type ProducerId = Int
  type SequenceId = Int

  trait QueryExecutor {
    def execute(userUID: String, query: String): JValue
    def startup: Future[Unit]
    def shutdown: Future[Unit]
  }

  trait NullQueryExecutor extends QueryExecutor {
    def actorSystem: ActorSystem
    implicit def executionContext: ExecutionContext

    def execute(userUID: String, query: String) = JString("Query service not avaialble")
    def startup = Future(())
    def shutdown = Future { actorSystem.shutdown }
  }

}


// vim: set ts=4 sw=4 et:
