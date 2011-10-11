package com.reportgrid.quirrel

import edu.uwm.cs.gll.LineStream

import jline.ConsoleReader
import jline.Terminal

import parser._

trait REPL extends Parser {
  val Prompt = "quirrel> "
  val Follow = "       | "
  
  def run() {
    Terminal.setupTerminal().initializeTerminal()
    
    val reader = new ConsoleReader
    
    var next = readNext(reader)
    while (!next.trim.startsWith(":q")) {
      val printTree = if (next.startsWith(":tree")) {
        next = next.substring(":tree".length)
        true
      } else {
        false
      }
      
      try {
        val tree = parse(LineStream(next))
        if (printTree) {
          println(tree)
        }
      } catch {
        case e @ ParseException(failures) => 
          println(e.mkString)
      }
      next = readNext(reader)
    }
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
}

object Console extends App {
  val repl = new REPL {}
  repl.run()
}
