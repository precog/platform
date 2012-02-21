package com.precog

import blueeyes.json.JsonAST._

import akka.actor.ActorSystem
import akka.dispatch.{Future, ExecutionContext, MessageDispatcher}

package object shard {

  trait QueryExecutor { 
    def execute(query: String): JValue
    def startup: Future[Unit]
    def shutdown: Future[Unit]
  }

  trait NullQueryExecutor extends QueryExecutor {
    def actorSystem: ActorSystem    
    implicit def executionContext: ExecutionContext
  
    def execute(query: String) = JString("Query service not avaialble")
    def startup = Future(())
    def shutdown = Future { actorSystem.shutdown }
  }

}
