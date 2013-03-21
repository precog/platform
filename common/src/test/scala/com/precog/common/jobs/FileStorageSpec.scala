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
package com.precog.common
package jobs

import blueeyes.core.http._
import blueeyes.core.http.MimeTypes._

import org.specs2.mutable._

import java.nio._

import scalaz._
import scalaz.Id.Id
import scalaz.syntax.monad._
import scalaz.syntax.comonad._

class InMemoryFileStorageSpec extends Specification {
  include(new FileStorageSpec[Need] {
    val M: Monad[Need] with Comonad[Need] = Need.need
    val fs = new InMemoryFileStorage[Need]
  })
}

trait FileStorageSpec[M[+_]] extends Specification {
  lazy val TEXT = MimeTypes.text / plain
  lazy val HTML = MimeTypes.text / html

  implicit def M: Monad[M] with Comonad[M]
  def fs: FileStorage[M]

  lazy val data1: FileData[M] = {
    val strings = "Hello" :: "," :: " " :: "world!" :: StreamT.empty[M, String]
    val data = strings map { s => s.getBytes("UTF-8") }
    FileData(Some(TEXT), data)
  }

  lazy val data2: FileData[M] = {
    val strings = "Goodbye" :: "," :: " " :: "cruel world." :: StreamT.empty[M, String]
    val data = strings map { s => s.getBytes("UTF-8") }
    FileData(Some(HTML), data)
  }

  def encode(s: StreamT[M, Array[Byte]]): M[String] = s.foldLeft("") { (acc, bytes) =>
    acc + new String(bytes, "UTF-8")
  }

  "File storage" should {
    "save (and load) arbitrary file" in {
      fs.save("f1", data1).copoint must not(throwA[Exception])
      fs.load("f1").copoint must beLike {
        case Some(FileData(Some(TEXT), data)) =>
          encode(data).copoint must_== "Hello, world!"
      }
    }

    "allow files to be overwritten" in {
      val file = for {
        _ <- fs.save("f2", data1)
        _ <- fs.save("f2", data2)
        file <- fs.load("f2")
      } yield file

      file.copoint must beLike {
        case Some(FileData(Some(HTML), data)) =>
          encode(data).copoint must_== "Goodbye, cruel world."
      }
    }

    "return None when loading a non-existent file" in {
      fs.load("I do not exists!").copoint must_== None
    }

    "say a file exists when its been saved" in {
      (for {
        _ <- fs.save("f3", data1)
        exists <- fs.exists("f3")
      } yield exists).copoint must_== true
    }

    "not pretend files exist when they don't" in {
      fs.exists("non-existant file").copoint must_== false
    }

    "allow removal of files" in {
      val result = for {
        _ <- fs.save("f4", data1)
        e1 <- fs.exists("f4")
        _ <- fs.remove("f4")
        e2 <- fs.exists("f4")
      } yield { e1 -> e2 }

      result.copoint must beLike { case (true, false) => ok }
    }
  }
}
