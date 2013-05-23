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
import com.precog.muspelheim.ParseEvalStackSpecs
import com.precog.util.IOUtils
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.vfs._

import com.weiglewilczek.slf4s.Logging

import java.util.UUID

import org.specs2.execute.EventuallyResults
import org.specs2.mutable._
import org.specs2.specification.Fragments

import scalaz._
import scalaz.syntax.comonad._

class NIHDBFileStoreSpec extends Specification with Logging {
  val tmpDir = IOUtils.createTmpDir("filestorespec").unsafePerformIO

  logger.info("Running NIHDBFileStoreSpec under " + tmpDir)

  val projectionSystem = new NIHDBPlatformSpecsActor(Some(tmpDir.getCanonicalPath))

  val projectionsActor = projectionSystem.actor

  val actorSystem = projectionSystem.actorSystem.get

  implicit val M: Monad[Future] with Comonad[Future] = new blueeyes.bkka.UnsafeFutureComonad(actorSystem.dispatcher, Duration(5, "Seconds"))

  implicit val timeout = new Timeout(5000)

  val loremIpsum = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed ac dolor ac velit consequat vestibulum at id dolor. Vivamus luctus mauris ac massa iaculis a cursus leo porta. Aliquam tellus ligula, mattis quis luctus sed, tempus id ante. Donec sagittis, ante pharetra tempor ultrices, purus massa tincidunt neque, ut tempus massa nisl non libero. Aliquam tincidunt commodo facilisis. Phasellus accumsan dapibus lorem ac aliquam. Nullam vitae ullamcorper risus. Praesent quis tellus lectus."

  import ParseEvalStackSpecs._

  "NIHDBPlatform storage" should {
    "Properly store and retrieve files" in {
      val testPath = Path("/store/this/somewhere")

      (projectionsActor ? IngestData(Seq((0L, StoreFileMessage(testAPIKey, testPath, Authorities(testAccount), None, EventId.fromLong(42L), FileContent(loremIpsum.getBytes("UTF-8"), MimeType("text", "plain"), RawUTF8Encoding), Clock.System.instant, StreamRef.Create(UUID.randomUUID, true)))))).copoint must beLike {
        case UpdateSuccess(_) => ok
      }

      (projectionsActor ? Read(testPath, Version.Current)).mapTo[ReadResult].copoint must beLike {
        case ReadSuccess(_, blob : BlobResource) => blob.asString.unsafePerformIO mustEqual loremIpsum
      }
    }

    "Properly handle atomic version updates" in {
      import Resource._
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
    projectionSystem.release
    IOUtils.recursiveDelete(tmpDir).unsafePerformIO
  }
}
