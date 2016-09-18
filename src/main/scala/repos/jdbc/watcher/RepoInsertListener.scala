package repos.jdbc.watcher

import java.nio.ByteBuffer

import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener
import com.github.shyiko.mysql.binlog.event._
import org.xerial.snappy.Snappy
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

case class BinLogPosition(filename: String, position: Long = 0, nextPosition: Long = 0)

case class RepoEntry(
  tableName: String,   // repo name
  pk: Long,
  uuid: ByteBuffer,  // representing the uuid
  timeMsec: Long,
  payload: ByteBuffer)

case class RepoInsertEvent(position: Option[BinLogPosition], entries: Seq[RepoEntry])

/** Implements an EventListener that calls a callback any time an insert to a repo
  * is observed. All the calls to the callback will happen from the same thread.
  * It is safe to block in onInsert in order to slow down production of new records.
  */
class RepoInsertListener(onInsert: RepoInsertEvent => Unit) extends EventListener {
  val logger = LoggerFactory.getLogger(this.getClass)
  val tableNames = collection.mutable.Map[Long, String]()
  var logFilename: Option[String] = None

  override def onEvent(event: Event): Unit = {
    val eventData: EventData = event.getData()
    val eventHeader = event.getHeader.asInstanceOf[EventHeaderV4]
    eventData match {
      case e: TableMapEventData =>
        tableNames += (e.getTableId -> e.getTable)
      case e: RotateEventData =>
        logFilename = Some(e.getBinlogFilename)
      case e: WriteRowsEventData if tableNames.contains(e.getTableId) =>
        if (logFilename.isEmpty) {
          logger.error("Empty log filename!")
          sys.exit(1)
        }
        val tableName = tableNames(e.getTableId)
        val entries = for {
          row@Array(pk: java.lang.Long, uuid: Array[Byte], timeMsec: java.lang.Long, format: Array[Byte], entryBin: Array[Byte]) <- e.getRows().asScala
        } yield {
          val fmtString = new String(format)
          val payload = fmtString match {
            case "1" => entryBin
            case "2" => Snappy.uncompress(entryBin)
            case _ =>
              logger.error("Invalid row format.")
              throw new RuntimeException("Invalid row format.")
          }
          RepoEntry(
            tableName = tableName,
            pk = pk,
            uuid = ByteBuffer.wrap(uuid),
            timeMsec = timeMsec,
            payload = ByteBuffer.wrap(payload))
        }

        if (entries.nonEmpty) {
          val insertRecord = RepoInsertEvent(
            position = Some(BinLogPosition(
              filename = logFilename.get,
              position = eventHeader.getPosition,
              nextPosition = eventHeader.getNextPosition)),
            entries = entries)
          onInsert(insertRecord)
        }
      case e: WriteRowsEventData =>
        logger.error(s"Miss on ${e.getTableId}")
      case _ =>
    }
  }
}
