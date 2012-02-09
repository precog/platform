package com.precog
package pandora

import akka.dispatch.Await
import akka.util.Duration

import com.precog.yggdrasil.SValue
import com.precog.yggdrasil.BaseConfig
import com.precog.yggdrasil.shard.YggState
import com.precog.yggdrasil.shard.ActorYggShard

import edu.uwm.cs.gll.{Failure, LineStream, Success}

import jline.TerminalFactory
import jline.console.ConsoleReader

import daze._
import quirrel.LineErrors
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import java.io.{File, PrintStream}

import scalaz.effect.IO

import net.lag.configgy.Configgy
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
    with DatasetConsumers 
    with OperationsAPI {

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
      val phaseErrors = runPhasesInSequence(tree)
      val allErrors = tree.errors ++ phaseErrors
      
      val strs = for (error <- allErrors) yield showError(error)
      
      if (!tree.errors.isEmpty || !phaseErrors.isEmpty) {
        out.println(color.red(strs mkString "\n"))
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
  

    out.println("Welcome to Quirrel version 0.0.1.")
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
  // Configuration required for blueyes IngestServer
  val controlTimeout = Duration(120, "seconds")
  Configgy.configureFromResource("default_ingest.conf")

  def loadConfig(dataDir: Option[String]): IO[BaseConfig with YggEnumOpsConfig with LevelDBQueryConfig] = IO {
    val rawConfig = dataDir map { "precog.storage.root = " + _ } getOrElse { "" }

    new BaseConfig with YggEnumOpsConfig with LevelDBQueryConfig {
      val config = Configuration.parse(rawConfig)  
      val flatMapTimeout = controlTimeout
      val projectionRetrievalTimeout = akka.util.Timeout(controlTimeout)
    }
  }

  val repl: IO[scalaz.Validation[blueeyes.json.xschema.Extractor.Error, Lifecycle]] = for {
    yconfig <- loadConfig(args.headOption) 
    shard  <- YggState.restore(yconfig.dataDir) 
  } yield {
    shard map { shardState => 
      new REPL 
          with AkkaIngestServer 
          with YggdrasilEnumOpsComponent
          with LevelDBQueryComponent
          with Lifecycle { self =>

        type YggConfig = YggEnumOpsConfig with LevelDBQueryConfig
        val yggConfig = yconfig

        val maxEvalDuration = controlTimeout

        object storage extends ActorYggShard {
          val yggState = shardState 
        }

        object ops extends Ops 

        object query extends QueryAPI 

        def startup = IO {
          // start ingest server
          Await.result(start, controlTimeout)
          // start storage shard 
          Await.result(storage.start, controlTimeout)
        }

        def shutdown = IO {
          // stop storaget shard
          Await.result(storage.stop, controlTimeout)
          // stop ingest server
          Await.result(stop, controlTimeout)

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
