package com.precog.muspelheim

import com.precog.bytecode._
import com.precog.common.json.CPath
import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.util._
import com.precog.util._
import SValue._

import blueeyes.json._

import akka.actor._
import akka.dispatch._
import akka.testkit.TestActorRef
import akka.util.Timeout
import akka.util.duration._

import java.io._
import org.reflections._
import scalaz._
import scalaz.effect.IO
import scalaz.std.AllInstances._
import scalaz.std.list._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap

import TableModule._

trait RawJsonStorageModule[M[+_]] extends StorageMetadataSource[M] { self =>
  implicit def M: Monad[M]

  protected def projectionData(descriptor: ProjectionDescriptor) = {
    if (!projections.contains(descriptor)) descriptor.columns.map(_.path).distinct.foreach(load)
    projections(descriptor)
  }

  def userMetadataView(apiKey: APIKey) = new UserMetadataView(apiKey, new UnrestrictedAccessControl[M](), metadata)

  private implicit val ordering = IdentitiesOrder.toScalaOrdering

  private val routingTable: RoutingTable = new SingleColumnProjectionRoutingTable

  private val identity = new java.util.concurrent.atomic.AtomicInteger(0)
  private var projections: Map[ProjectionDescriptor, Vector[JValue]] = Map() 

  private def load(path: Path) = {
    val resourceName = ("/test_data" + path.toString.init + ".json").replaceAll("/+", "/")   

    using(getClass.getResourceAsStream(resourceName)) { in =>
      // FIXME: Refactor as soon as JParser can parse from InputStreams
      val reader = new InputStreamReader(in)
      val buffer = new Array[Char](8192)
      val builder = new java.lang.StringBuilder
      var read = 0
      do {
        read = reader.read(buffer)
        if (read >= 0) {
          builder.append(buffer, 0, read)
        }
      } while (read >= 0)
      
      val json = JParser.parse(builder.toString) --> classOf[JArray]

      projections = json.elements.foldLeft(projections) { 
        case (acc, jobj) => 
          val evID = EventId(0, identity.getAndIncrement)
          routingTable.routeIngest(IngestMessage("", path, "", Seq(IngestRecord(evID, jobj)), None)).foldLeft(acc) {
            case (acc, data) =>
              acc + (data.descriptor -> (acc.getOrElse(data.descriptor, Vector.empty[JValue]) ++ data.toJValues))
          }
      }
    }
  }

  private val projectionMetadata: Map[ProjectionDescriptor, ColumnMetadata] = {
    import org.reflections.util._
    import org.reflections.scanners._
    import scala.collection.JavaConverters._
    import java.util.regex.Pattern

    val reflections = new Reflections(new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage("test_data")).setScanners(new ResourcesScanner()))
    val jsonFiles = reflections.getResources(Pattern.compile(".*\\.json"))
    for (resource <- jsonFiles.asScala) load(Path(resource.replaceAll("test_data/", "").replaceAll("\\.json", "")))

    projections.keys.map(pd => (pd, ColumnMetadata.Empty)).toMap
  }

  private val metadata = new StorageMetadata[M] {
    val M = self.M
    val source = new TestMetadataStorage(projectionMetadata)
    def findChildren(path: Path) = M.point(source.findChildren(path))
    def findSelectors(path: Path) = M.point(source.findSelectors(path))
    def findProjections(path: Path, selector: CPath) = M.point {
      projections.collect {
        case (descriptor, _) if descriptor.columns.exists { case ColumnDescriptor(p, s, _, _) => p == path && s == selector } => 
          (descriptor, ColumnMetadata.Empty)
      }
    }

    def findPathMetadata(path: Path, selector: CPath) = M.point(source.findPathMetadata(path, selector).unsafePerformIO)
  }
}

trait RawJsonColumnarTableStorageModule[M[+_]] extends RawJsonStorageModule[M] with ColumnarTableModuleTestSupport[M] {
  import trans._
  import TableModule._

  trait TableCompanion extends ColumnarTableCompanion {
    def apply(slices: StreamT[M, Slice], size: TableSize = UnknownSize) = new Table(slices, size)
    def align(sourceLeft: Table, alignOnL: TransSpec1, sourceRight: Table, alignOnR: TransSpec1): M[(Table, Table)] = sys.error("Feature not implemented in test stub.")
    // FIXME: There should be some way to make SingletonTable work here, too
    def singleton(slice: Slice) = new Table(slice :: StreamT.empty[M, Slice], ExactSize(1))
  }
  
  class Table(slices: StreamT[M, Slice], size: TableSize) extends ColumnarTable(slices, size) {
    import trans._
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean = false) = sys.error("Feature not implemented in test stub")
    
    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder = SortAscending, unique: Boolean = false): M[Seq[Table]] = sys.error("Feature not implemented in test stub.")

    def load(apiKey: APIKey, tpe: JType): M[Table] = {
      val pathsM = this.reduce {
        new CReducer[Set[Path]] {
          def reduce(schema: CSchema, range: Range): Set[Path] = {
            schema.columns(JObjectFixedT(Map("value" -> JTextT))) flatMap {
              case s: StrColumn => range.filter(s.isDefinedAt).map(i => Path(s(i)))
              case _ => Set()
            }
          }
        }
      }

      for {
        paths <- pathsM
        data  <- paths.toList.map(userMetadataView(apiKey).findProjections).sequence
      } yield {
        data.flatten.headOption map {
          case (descriptor, _) => fromJson(projectionData(descriptor).toStream) 
        } getOrElse {
          Table.empty
        }
      }
    }
  }
}



// vim: set ts=4 sw=4 et:
