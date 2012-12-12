package com.precog
package pandora

import akka.actor.ActorSystem
import akka.dispatch._
import akka.util.Duration

import blueeyes.json._

import com.codecommit.gll.{Failure, LineStream, Success}

import jline.TerminalFactory
import jline.console.ConsoleReader


import com.precog.accounts.InMemoryAccountManager
import com.precog.common.Path
import com.precog.common.kafka._
import com.precog.common.security._
import com.precog.util.PrecogUnit

import yggdrasil._
import yggdrasil.actor._
import yggdrasil.jdbm3._
import yggdrasil.metadata._
import yggdrasil.serialization._
import yggdrasil.table._
import yggdrasil.table.jdbm3._
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
  def startup: IO[PrecogUnit]
  def run: IO[PrecogUnit]
  def shutdown: IO[PrecogUnit]
}

trait REPL
    extends muspelheim.ParseEvalStack[Future] 
    with IdSourceScannerModule[Future]
    with MemoryDatasetConsumer[Future] {

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
  val controlTimeout = Duration(120, "seconds")
  class REPLConfig(dataDir: Option[String]) extends 
      BaseConfig with
      IdSourceConfig with
      StandaloneShardSystemConfig with
      ColumnarTableModuleConfig with
      BlockStoreColumnarTableModuleConfig with
      JDBMProjectionModuleConfig {
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
          with JDBMColumnarTableModule[Future]
          with JDBMProjectionModule
          with SystemActorStorageModule
          with StandaloneShardSystemActorModule { self =>

        trait TableCompanion extends JDBMColumnarTableCompanion {
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
          val accessControl = new UnrestrictedAccessControl[Future]()
          val accountManager = new InMemoryAccountManager[Future]()
        }

        val storage = new Storage

        object Projection extends JDBMProjectionCompanion {
          val fileOps = FilesystemFileOps
          def baseDir(descriptor: ProjectionDescriptor) = sys.error("todo")
          def archiveDir(descriptor: ProjectionDescriptor) = sys.error("todo")
        }

        def startup = IO { Await.result(storage.start(), controlTimeout); PrecogUnit }

        def shutdown = IO { 
          Await.result(storage.stop(), controlTimeout) 
          actorSystem.shutdown
          PrecogUnit
        }
      })

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
