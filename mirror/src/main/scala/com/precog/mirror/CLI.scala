package com.precog.mirror

import blueeyes.json._
import scalaz._
import java.io.File

object CLI extends App with EvaluatorModule {
  val config = processArgs(args.toList)
  
  val forest = compile(config('query))
  val filtered = forest filter { _.errors filterNot isWarning isEmpty }
  
  if (filtered.size == 1) {
    filtered flatMap { _.errors filter isWarning } map showError foreach System.err.println
    
    eval(filtered.head)(load) foreach { jv =>
      println(jv.renderCompact)
    }
  } else if (filtered.size > 1) {
    System.err.println("ambiguous compilation results")
    System.exit(-1)
  } else {
    val out = forest map { tree =>
      tree.errors map showError mkString "\n\n"
    } mkString "\n-------------\n"
    System.err.println(out)
    System.exit(-1)
  }
  
  lazy val stdin: Stream[JValue] = {
    def gen: Stream[JValue] = {
      val line = Console.readLine
      if (line != null) {
        val result = JParser.parseFromString(line) match {
          case Success(jv) => jv
          case Failure(t) => throw t
        }
        result #:: gen
      } else {
        Stream.empty
      }
    }
    
    gen
  }
  
  def load(path: String): Seq[JValue] = {
    val basePath = config get 'base getOrElse "."
    
    if (path == "-") {
      stdin
    } else {
      val file = new File(basePath + path)
      
      JParser.parseManyFromFile(file) match {
        case Success(seq) => seq
        case Failure(t) => throw t
      }
    }
  }
  
  def processArgs(args: List[String]): Map[Symbol, String] = args match {
    // case "--inf" :: format :: tail => processArgs(tail) + ('inf -> format)
    // case "--outf" :: format :: tail => processArgs(tail) + ('outf -> format)
    
    case "--base" :: basePath :: tail => processArgs(tail) + ('base -> basePath)
    
    case query :: Nil => Map('query -> query)
  }
}
