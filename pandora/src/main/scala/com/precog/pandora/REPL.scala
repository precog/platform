package com.precog
package pandora

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext
import akka.dispatch.Await
import akka.util.Duration

import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.common.kafka._
import blueeyes.json.Printer._
import blueeyes.json.JsonAST._

import com.codecommit.gll.{Failure, LineStream, Success}

import jline.TerminalFactory
import jline.console.ConsoleReader

import daze._
import quirrel.LineErrors
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import java.io.{File, PrintStream}

import scalaz.effect.IO

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

trait Lifecycle {
  def startup: IO[Unit]
  def run: IO[Unit]
  def shutdown: IO[Unit]
}

trait REPL extends LineErrors
    with Parser
    with TreeShaker
    with ProvenanceChecker
    with Emitter
    with Evaluator
    with StdLib
    with MemoryDatasetConsumer 
    with OperationsAPI {

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
            val result = consumeEval(dummyUID, graph) fold (
              error   => "An error occurred processing your query: " + error.getMessage,
              results => pretty(render(JArray(results.toList.map(_._2.toJValue))))
            )
            
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
      YggEnumOpsConfig with 
      LevelDBQueryConfig with 
      DiskMemoizationConfig with 
      DatasetConsumersConfig with 
      ProductionActorConfig {
    val defaultConfig = Configuration.loadResource("/default_ingest.conf", BlockFormat)
    val config = dataDir map { defaultConfig.set("precog.storage.root", _) } getOrElse { defaultConfig }

    val flatMapTimeout = controlTimeout
    val projectionRetrievalTimeout = akka.util.Timeout(controlTimeout)
    val sortWorkDir = scratchDir
    val chunkSerialization = BinaryProjectionSerialization
    val memoizationBufferSize = sortBufferSize
    val memoizationWorkDir = scratchDir
    val maxEvalDuration = controlTimeout
  }

  def loadConfig(dataDir: Option[String]): IO[REPLConfig] = IO {
    new REPLConfig(dataDir)
  }

  val repl: IO[scalaz.Validation[blueeyes.json.xschema.Extractor.Error, Lifecycle]] = for {
    replConfig <- loadConfig(args.headOption) 
    replState <- YggState.restore(replConfig.dataDir) 
  } yield {
    replState map { shardState => 
      new REPL 
          with YggdrasilEnumOpsComponent
          with LevelDBQueryComponent
          with DiskMemoizationComponent
          with Lifecycle { self =>

        lazy val actorSystem = ActorSystem("repl_actor_system")
        implicit lazy val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

        type YggConfig = REPLConfig
        val yggConfig = replConfig

        type Storage = ActorYggShard
        val storage = new ActorYggShard with StandaloneActorEcosystem {
          type YggConfig = REPLConfig
          lazy val yggConfig = replConfig
          lazy val yggState = shardState
        }

        object ops extends Ops 

        object query extends QueryAPI 

        def startup = IO { Await.result(storage.actorsStart, controlTimeout) }

        def shutdown = IO { 
          Await.result(storage.actorsStop, controlTimeout) 
          actorSystem.shutdown
        }
      }
    }
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
