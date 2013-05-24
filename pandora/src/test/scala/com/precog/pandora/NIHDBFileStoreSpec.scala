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
package com.precog.pandora

import akka.dispatch.Future
import akka.pattern.ask
import akka.util.{Duration, Timeout}

import blueeyes.core.http.MimeType
import blueeyes.json._
import blueeyes.util.Clock

import com.precog.common.Path
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.muspelheim._
import com.precog.util.IOUtils
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.vfs._

import com.weiglewilczek.slf4s.Logging
import org.streum.configrity.Configuration

import java.util.UUID

import org.specs2.execute.EventuallyResults
import org.specs2.mutable._
import org.specs2.specification.Fragments

import scalaz._
import scalaz.syntax.comonad._

class NIHDBFileStoreSpec extends NIHDBTestActors with Specification with Logging {
  val tmpDir = IOUtils.createTmpDir("filestorespec").unsafePerformIO
  class YggConfig extends NIHDBTestActorsConfig {
    val config = Configuration parse { "precog.storage.root = %s".format(tmpDir) }
    val clock = blueeyes.util.Clock.System
    val maxSliceSize = 10
  } 

  object yggConfig extends YggConfig

  logger.info("Running NIHDBFileStoreSpec under " + tmpDir)

  implicit val timeout = new Timeout(5000)

  val loremIpsum = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed ac dolor ac velit consequat vestibulum at id dolor. Vivamus luctus mauris ac massa iaculis a cursus leo porta. Aliquam tellus ligula, mattis quis luctus sed, tempus id ante. Donec sagittis, ante pharetra tempor ultrices, purus massa tincidunt neque, ut tempus massa nisl non libero. Aliquam tincidunt commodo facilisis. Phasellus accumsan dapibus lorem ac aliquam. Nullam vitae ullamcorper risus. Praesent quis tellus lectus."

  import TestStack._

  "NIHDBPlatform storage" should {
    "Properly store and retrieve files" in {
      val testPath = Path("/store/this/somewhere")

      (projectionsActor ? IngestData(Seq((0L, StoreFileMessage(testAPIKey, testPath, Authorities(testAccount), None, EventId.fromLong(42L), FileContent(loremIpsum.getBytes("UTF-8"), MimeType("text", "plain"), RawUTF8Encoding), Clock.System.instant, StreamRef.Create(UUID.randomUUID, true)))))).copoint must beLike {
        case UpdateSuccess(_) => ok
      }

      (projectionsActor ? Read(testPath, Version.Current)).mapTo[ReadResult].copoint must beLike {
        case ReadSuccess(_, blob : BlobResource) => blob.asString.run.copoint must beSome(loremIpsum)
      }
    }

    "Properly handle atomic version updates" in {
      import ResourceError._
      val testPath = Path("/versioned/blob")

      val streamId = UUID.randomUUID

      (projectionsActor ? IngestData(Seq((0L, IngestMessage(testAPIKey, testPath, Authorities(testAccount), Seq(IngestRecord(EventId.fromLong(42L), JString("Foo!"))), None, Clock.System.instant, StreamRef.Create(streamId, false)))))).copoint must beLike {
        case UpdateSuccess(_) => ok
      }

      // We haven't terminated the stream yet, so it shouldn't find anything
      (projectionsActor ? Read(testPath, Version.Current)).mapTo[ReadResult].copoint must beLike {
        case PathOpFailure(_, NotFound(_)) => ok
      }

      (projectionsActor ? IngestData(Seq((1L, IngestMessage(testAPIKey, testPath, Authorities(testAccount), Seq(IngestRecord(EventId.fromLong(42L), JString("Foo!"))), None, Clock.System.instant, StreamRef.Create(streamId, true)))))).copoint must beLike {
        case UpdateSuccess(_) => ok
      }

      (projectionsActor ? Read(testPath, Version.Current)).mapTo[ReadResult].copoint must beLike {
        case ReadSuccess(_, proj: NIHDBResource) => proj.db.length.copoint mustEqual 2
      }
    }
  }

  override def map(fs: => Fragments): Fragments = fs ^ step {
    logger.info("Unlocking actor")
    //projectionSystem.release
    IOUtils.recursiveDelete(tmpDir).unsafePerformIO
  }
}
