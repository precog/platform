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

import akka.actor.ActorSystem
import akka.dispatch._
import akka.util.Duration

import blueeyes.json.Printer._
import blueeyes.json.JsonAST._

import com.codecommit.gll.{Failure, LineStream, Success}

import jline.TerminalFactory
import jline.console.ConsoleReader

import com.precog.common.Path

import com.precog.common.kafka._
import com.precog.common.security._
import yggdrasil._
import yggdrasil.actor._
import yggdrasil.jdbm3._
import yggdrasil.metadata._
import yggdrasil.serialization._
import yggdrasil.table._
import yggdrasil.util._

import daze._

import quirrel.LineErrors
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import com.precog.util.FilesystemFileOps

import java.io.{File, PrintStream}

import scalaz._
import scalaz.effect.IO

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

trait Lifecycle {
  def startup: IO[Unit]
  def run: IO[Unit]
  def shutdown: IO[Unit]
}

trait REPL
    extends muspelheim.ParseEvalStack[Future] 
    with IdSourceScannerModule[Future]
    with MemoryDatasetConsumer[Future] {

  val dummyUID = "dummyUID"

  val Prompt = "quirrel> "
  val Follow = "       | "

  def run = IO {
    val terminal = TerminalFactory.getFlavor(TerminalFactory.Flavor.UNIX)
    terminal.init()
    
    val color = new Color(true)       // TODO   
    
    val reader = new ConsoleReader
    // val out = new PrintWriter(reader.getTerminal.wrapOutIfNeeded(System.out))
    val out = System.out
    
    def compile(oldTree: Expr): Option[Expr] = {
      bindRoot(oldTree, oldTree)
      
      val tree = shakeTree(oldTree)
      val strs = for (error <- tree.errors) yield showError(error)
      
      if (!tree.errors.isEmpty) {
        out.println(color.red(strs mkString "\n"))
      }
      
      if (tree.errors filterNot isWarning isEmpty)
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
            val result = withContext { ctx =>
              consumeEval(dummyUID, graph, ctx,Path.Root) fold (
                error   => "An error occurred processing your query: " + error.getMessage,
                results => pretty(render(JArray(results.toList.map(_._2.toJValue))))
              )
            }
            
            out.println()
            out.println(color.cyan(result))
          }
        }
        
        true
      }
      
      case PrintTree(tree) => {
        bindRoot(tree, tree)
        val tree2 = shakeTree(tree)
        
        out.println()
        out.println(prettyPrint(tree2))
        
        true
      }
      
      case Help => {
        printHelp(out)
        true
      }
        
      case Quit => {
        terminal.restore()
        false
      }
    }
    
    def loop() {
      val results = prompt(readNext(reader, color))
      val successes = results collect { case Success(tree, _) => tree }
      val failures = results collect { case f: Failure => f }
      
      if (successes.isEmpty) {
        try {
          handleFailures(failures)
        } catch {
          case pe: ParseException => {
            out.println()
            out.println(color.red(pe.mkString))
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
          out.println()
          loop()
        }
      }
    }
  

    out.println("Welcome to Quirrel early access preview.")       // TODO we should try to get this string from a file
    out.println("Type in expressions to have them evaluated.")
    out.println("Press Ctrl-D on a new line to evaluate an expression.")
    out.println("Type in :help for more information.")
    out.println()
  
    loop()
  }

  def readNext(reader: ConsoleReader, color: Color): String = {
    var input = reader.readLine(color.blue(Prompt))
    if (input == null) {
      readNext(reader, color)
    } else {
      var line = reader.readLine(color.blue(Follow))
      while (line != null) {
        input += '\n' + line
        line = reader.readLine(color.blue(Follow))
      }
      println()
      input.trim
    }
  }
  
  def printHelp(out: PrintStream) {
    val str = 
      """Note: command abbreviations are not yet supported!
        |
        |<expr>        Evaluate the expression
        |:help         Print this help message
        |:quit         Exit the REPL
        |:tree <expr>  Print the AST for the expression"""
        
    out.println(str stripMargin '|')
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
  val controlTimeout = Duration(120, "seconds")
  class REPLConfig(dataDir: Option[String]) extends 
      BaseConfig with 
      EvaluatorConfig with
      StandaloneShardSystemConfig {
    val defaultConfig = Configuration.loadResource("/default_ingest.conf", BlockFormat)
    val config = dataDir map { defaultConfig.set("precog.storage.root", _) } getOrElse { defaultConfig }

    val sortWorkDir = scratchDir
    val memoizationBufferSize = sortBufferSize
    val memoizationWorkDir = scratchDir

    val flatMapTimeout = controlTimeout
    val projectionRetrievalTimeout = akka.util.Timeout(controlTimeout)
    val maxEvalDuration = controlTimeout
    val clock = blueeyes.util.Clock.System
    
    val maxSliceSize = 10000

    //TODO: Get a producer ID
    val idSource = new IdSource {
      private val source = new java.util.concurrent.atomic.AtomicLong
      def nextId() = source.getAndIncrement
    }
  }

  def loadConfig(dataDir: Option[String]): IO[REPLConfig] = IO {
    new REPLConfig(dataDir)
  }

  val repl: IO[scalaz.Validation[blueeyes.json.serialization.Extractor.Error, Lifecycle]] = for {
    replConfig <- loadConfig(args.headOption) 
    fileMetadataStorage <- FileMetadataStorage.load(replConfig.dataDir, replConfig.archiveDir, FilesystemFileOps)
  } yield {
      scalaz.Success[blueeyes.json.serialization.Extractor.Error, Lifecycle](new REPL 
          with Lifecycle 
          with BlockStoreColumnarTableModule[Future]
          with JDBMProjectionModule
          with SystemActorStorageModule
          with StandaloneShardSystemActorModule { self =>

        trait TableCompanion extends BlockStoreColumnarTableCompanion {
          import scalaz.std.anyVal._
          implicit val geq: scalaz.Equal[Int] = scalaz.Equal[Int]
        }

        object Table extends TableCompanion

        val actorSystem = ActorSystem("replActorSystem")
        implicit val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

        type YggConfig = REPLConfig
        val yggConfig = replConfig

        implicit val M: Monad[Future] with Copointed[Future] = new blueeyes.bkka.FutureMonad(asyncContext) with Copointed[Future] {
          def copoint[A](m: Future[A]) = Await.result(m, yggConfig.maxEvalDuration)
        }

        class Storage extends SystemActorStorageLike(fileMetadataStorage) {
          val accessControl = new UnlimitedAccessControl[Future]()
        }

        val storage = new Storage

        object Projection extends JDBMProjectionCompanion {
          val fileOps = FilesystemFileOps
          def baseDir(descriptor: ProjectionDescriptor) = sys.error("todo")
          def archiveDir(descriptor: ProjectionDescriptor) = sys.error("todo")
        }

        def startup = IO { Await.result(storage.start(), controlTimeout) }

        def shutdown = IO { 
          Await.result(storage.stop(), controlTimeout) 
          actorSystem.shutdown
        }
      })

  }

  val run = repl.flatMap[Unit] {
    case scalaz.Success(lifecycle) => 
      for {
        _ <- lifecycle.startup
        _ <- lifecycle.run
        _ <- lifecycle.shutdown
      } yield ()

    case scalaz.Failure(error) =>
      IO(sys.error("An error occurred deserializing a database descriptor: " + error))
  }

  run.unsafePerformIO
}
