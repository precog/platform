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
package pandora

import com.precog.yggdrasil.SValue

import edu.uwm.cs.gll.{Failure, LineStream, Success}

import jline.{TerminalFactory, UnixTerminal}
import jline.console.ConsoleReader

import daze._
import yggdrasil.shard._

import akka.dispatch.Await
import akka.util.Duration
import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext

import java.util.Properties
import java.net.URLClassLoader

import quirrel.LineErrors
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import scalaz.effect.IO

trait REPL extends LineErrors
    with Parser
    with TreeShaker
    with ProvenanceChecker
    with Emitter
    with Evaluator
    with DefaultYggConfig
    with StorageShardModule
    with YggdrasilOperationsAPI
    with DatasetConsumers {

  val actorSystem = ActorSystem()
  implicit val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
 
  val Prompt = "quirrel> "
  val Follow = "       | "

  lazy val storage = storageShard.unsafePerformIO

  def run() {
    TerminalFactory.getFlavor(TerminalFactory.Flavor.UNIX).asInstanceOf[UnixTerminal].init()
    
    val reader = new ConsoleReader
    
    def compile(oldTree: Expr): Option[Expr] = {
      bindRoot(oldTree, oldTree)
      
      val tree = shakeTree(oldTree)
      val phaseErrors = runPhasesInSequence(tree)
      val allErrors = tree.errors ++ phaseErrors
      
      val strs = for (error <- allErrors) yield {
        showError(error) 
      }
      
      if (!tree.errors.isEmpty || !phaseErrors.isEmpty) {
        println(strs mkString "\n")
      }
      
      if (allErrors filterNot isWarning isEmpty)
        Some(tree)
      else
        None
    }
    
    def handle(c: Command) = c match {
      case Eval(tree) => {
        val optTree = compile(tree)
        
        for (tree <- optTree) {
          val bytecode = emit(tree)
          val eitherGraph = decorate(bytecode)
          
          // TODO decoration errors
          
          for (graph <- eitherGraph.right) {
            val result = consumeEval(graph) map { _._2 } map SValue.asJSON mkString ("[", ",", "]")
            
            println()
            println(result)
          }
        }
        
        true
      }
      
      case PrintTree(tree) => {
        bindRoot(tree, tree)
        val tree2 = shakeTree(tree)
        
        println()
        println(prettyPrint(tree2))
        
        true
      }
      
      case Help => { 
        printHelp()
        true
      }
        
      case Quit => false
    }
    
    def loop() {
      val results = prompt(readNext(reader))
      val successes = results collect { case Success(tree, _) => tree }
      val failures = results collect { case f: Failure => f }
      
      if (successes.isEmpty) {
        try {
          handleFailures(failures)
        } catch {
          case pe: ParseException => {
            println()
            println(pe.mkString)
          }
        }
        println()
        loop()
      } else {
        val command = if ((successes lengthCompare 1) > 0)
          throw new AssertionError("Fatal error: ambiguous parse results: " + results.mkString(", "))
        else
          successes.head
        
        if (handle(command)) {
          println()
          loop()
        }
      }
    }
   
    Await.result(storage.start, Duration(120, "seconds"))

    println("Welcome to Quirrel version 0.0.0.")
    println("Type in expressions to have them evaluated.")
    println("Press Ctrl-D on a new line to evaluate an expression.")
    println("Type in :help for more information.")
    println()
  
    loop()
    
    Await.result(storage.stop, Duration(120, "seconds"))

    actorSystem.shutdown
  }

  def storageShardConfig() = {
    val config = StorageShardModule.defaultProperties
    config.setProperty("precog.storage.root", "/tmp/repl_test_storage") 
    config.setProperty("precog.kafka.enable", "false")        // disable kafka consumer until we do an ingest test (requires zk and kafka) 
    config
  }

  def readNext(reader: ConsoleReader): String = {
    var input = reader.readLine(Prompt)
    if (input == null) {
      readNext(reader)
    } else {
      var line = reader.readLine(Follow)
      while (line != null) {
        input += '\n' + line
        line = reader.readLine(Follow)
      }
      println()
      input.trim
    }
  }
  
  def printHelp() {
    val str = 
      """Note: command abbreviations are not yet supported!
        |
        |<expr>        Evaluate the expression
        |:help         Print this help message
        |:quit         Exit the REPL
        |:tree <expr>  Print the AST for the expression"""
        
    println(str stripMargin '|')
  }
  
  // %%
  
  lazy val prompt: Parser[Command] = (
      expr           ^^ { t => Eval(t) }
    | ":tree" ~ expr ^^ { (_, t) => PrintTree(t) }
    | ":help"        ^^^ Help
    | ":quit"        ^^^ Quit
  )
  
  sealed trait Command
  
  case class Eval(tree: Expr) extends Command
  case class PrintTree(tree: Expr) extends Command
  case object Help extends Command
  case object Quit extends Command
}

object Console extends App {
  val repl = new REPL {}
  repl.run()
}
