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

import akka.actor.{ActorSystem, Props}
import akka.dispatch._
import akka.pattern.gracefulStop
import akka.util.{Duration, Timeout}

import blueeyes.bkka._
import blueeyes.json._

import com.codecommit.gll.{Failure, LineStream, Success}

import jline.TerminalFactory
import jline.console.ConsoleReader

import com.precog.common.Path
import com.precog.common.kafka._
import com.precog.common.accounts._
import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.niflheim._
import com.precog.util.PrecogUnit
import com.precog.util.FilesystemFileOps
import com.precog.yggdrasil.vfs._
import com.precog.yggdrasil.scheduling._

import yggdrasil._
import yggdrasil.actor._
import yggdrasil.metadata._
import yggdrasil.nihdb._
import yggdrasil.serialization._
import yggdrasil.table._
import yggdrasil.util._
import muspelheim._

import daze._

import quirrel.LineErrors
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import blueeyes.util.Clock

import java.io.{File, PrintStream}

import scalaz._
import scalaz.effect.IO

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

trait Lifecycle {
  def startup: IO[PrecogUnit]
  def run: IO[PrecogUnit]
  def shutdown: IO[PrecogUnit]
}

class REPLConfig(dataDir: Option[String]) extends BaseConfig
    with IdSourceConfig
    with EvaluatorConfig
    with ColumnarTableModuleConfig
    with BlockStoreColumnarTableModuleConfig {
  val defaultConfig = Configuration.loadResource("/default_ingest.conf", BlockFormat)
  val config = dataDir map { defaultConfig.set("precog.storage.root", _) } getOrElse { defaultConfig }

  val sortWorkDir = scratchDir
  val memoizationBufferSize = sortBufferSize
  val memoizationWorkDir = scratchDir
  val ingestConfig = None

  val storageTimeout = Timeout(Duration(120, "seconds"))
  val controlTimeout = storageTimeout.duration
  val flatMapTimeout = controlTimeout
  val maxEvalDuration = controlTimeout
  val clock = blueeyes.util.Clock.System

  val maxSliceSize = 20000
  val cookThreshold = maxSliceSize
  val smallSliceSize = 8

  //TODO: Get a producer ID
  val idSource = new FreshAtomicIdSource
}

trait REPL extends ParseEvalStack[Future]
    with NIHDBColumnarTableModule
    with LongIdMemoryDatasetConsumer[Future] {

  val dummyAPIKey = "dummyAPIKey"

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
            val result = {
              consumeEval(dummyAPIKey, graph, Path.Root) fold (
                error   => "An error occurred processing your query: " + error.getMessage,
                results => JArray(results.toList.map(_._2.toJValue)).renderPretty
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

    PrecogUnit
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
  def loadConfig(dataDir: Option[String]): IO[REPLConfig] = IO {
    new REPLConfig(dataDir)
  }

  val repl: IO[scalaz.Validation[blueeyes.json.serialization.Extractor.Error, Lifecycle]] = for {
    replConfig <- loadConfig(args.headOption)
  } yield {
    scalaz.Success[blueeyes.json.serialization.Extractor.Error, Lifecycle] {
      new REPL
          with Lifecycle { self =>
        val storageTimeout = yggConfig.storageTimeout

        implicit val actorSystem = ActorSystem("replActorSystem")
        implicit val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
        implicit val M = new blueeyes.bkka.UnsafeFutureComonad(asyncContext, yggConfig.maxEvalDuration)

        type YggConfig = REPLConfig
        val yggConfig = replConfig

        val accountFinder = None
        val apiKeyManager = new InMemoryAPIKeyManager[Future](yggConfig.clock)

        val accessControl = new DirectAPIKeyFinder(apiKeyManager)
        val permissionsFinder = new PermissionsFinder(accessControl, new StaticAccountFinder[Future]("", ""), new org.joda.time.Instant())

        val masterChef = actorSystem.actorOf(Props(Chef(VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)), VersionedSegmentFormat(Map(1 -> V1SegmentFormat)))))

        val resourceBuilder = new DefaultResourceBuilder(actorSystem, yggConfig.clock, masterChef, yggConfig.cookThreshold, storageTimeout, permissionsFinder)
        val projectionsActor = actorSystem.actorOf(Props(new PathRoutingActor(yggConfig.dataDir, resourceBuilder, permissionsFinder, Duration(300, "seconds"), new InMemoryJobManager[Future], yggConfig.clock)))

        val jobManager = new InMemoryJobManager[Future]
        val actorVFS = new ActorVFS(projectionsActor, yggConfig.storageTimeout, yggConfig.storageTimeout)
        val vfs = new SecureVFS(actorVFS, permissionsFinder, jobManager, NoopScheduler[Future], Clock.System)

        trait TableCompanion extends NIHDBColumnarTableCompanion

        object Table extends TableCompanion

        def Evaluator[N[+_]](N0: Monad[N])(implicit mn: Future ~> N, nm: N ~> Future): EvaluatorLike[N] =
          new Evaluator[N](N0) with IdSourceScannerModule {
            type YggConfig = REPLConfig
            val yggConfig = replConfig
            val report = LoggingQueryLogger[N](N0)
          }

        def startup = IO { PrecogUnit }

        def shutdown = IO {
          Await.result(gracefulStop(projectionsActor, yggConfig.controlTimeout), yggConfig.controlTimeout)
          actorSystem.shutdown()
          PrecogUnit
        }
      }
    }
  }

  val run = repl.flatMap[PrecogUnit] {
    case scalaz.Success(lifecycle) =>
      for {
        _ <- lifecycle.startup
        _ <- lifecycle.run
        _ <- lifecycle.shutdown
      } yield PrecogUnit

    case scalaz.Failure(error) =>
      IO(sys.error("An error occurred deserializing a database descriptor: " + error))
  }

  run.unsafePerformIO
}
