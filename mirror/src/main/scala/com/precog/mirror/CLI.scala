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
