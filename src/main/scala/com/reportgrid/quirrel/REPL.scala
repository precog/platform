package com.reportgrid.quirrel

import edu.uwm.cs.gll.Failure
import edu.uwm.cs.gll.LineStream
import edu.uwm.cs.gll.Success

import jline.ConsoleReader
import jline.Terminal

import parser._
import typer._

trait REPL extends Parser with Binder with LineErrors {
  val Prompt = "quirrel> "
  val Follow = "       | "
  
  def run() {
    Terminal.setupTerminal().initializeTerminal()
    
    val reader = new ConsoleReader
    
    def handle(c: Command) = c match {
      case Eval(tree) => {
        handleSuccesses(Stream(tree))      // a little nasty...
        true     // TODO
      }
      
      case PrintTree(tree) => {
        handleSuccesses(Stream(tree))      // a little nasty...
        val result = prettyPrint(tree)
        
        if (tree.errors.isEmpty)
          println(result)
        else
          println(tree.errors map showError mkString "\n")
        
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
          case pe: ParseException => println(pe.mkString)     // TODO
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
    
    println("Welcome to Quirrel version 0.0.0.")
    println("Type in expressions to have them evaluated (TODO test environment?).")
    println("All expressions must be followed by a single blank line.")
    println("Type in :help for more information.")
    println()
    
    loop()
  }
  
  def readNext(reader: ConsoleReader) = {
    var input = reader.readLine(Prompt)
    var line = reader.readLine(Follow)
    while (line.trim != "") {
      input += '\n' + line
      line = reader.readLine(Follow)
    }
    input
  }
  
  def printHelp() {
    val str = 
      """Note: command abbreviations are not yet supported!
        |
        |<expr>        Evaluate the expression
        |:help         Print this help message
        |:quit         Exit the REPL
        |:tree <expr>  Print the AST resulting from the parse phase (TODO subsequent phases)"""
        
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
