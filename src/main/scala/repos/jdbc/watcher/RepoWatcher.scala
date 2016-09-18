package repos.jdbc.watcher

import java.util
import java.util.concurrent.ArrayBlockingQueue

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.deserialization._
import com.github.shyiko.mysql.binlog.event.{EventType, TableMapEventData}


case class ConnectionParams(
  hostname: String,
  port: Int,
  username: String,
  password: String,
  position: Option[BinLogPosition])

object RepoWatcher {

  /** Creates a binary log client that is initialized with the given parameters */
  def createClient(params: ConnectionParams): BinaryLogClient = {
    val client = new BinaryLogClient(params.hostname, params.port, params.username, params.password)
    params.position.foreach {
      p =>
        client.setBinlogFilename(p.filename)
        if (p.position > 0) {
          client.setBinlogPosition(p.position)
        }
    }

    val tableMap = new util.HashMap[java.lang.Long, TableMapEventData]
    val eventMap = new util.HashMap[EventType, EventDataDeserializer[_]]
    eventMap.put(EventType.ROTATE, new RotateEventDataDeserializer)
    eventMap.put(EventType.TABLE_MAP, new TableMapEventDataDeserializer())
    eventMap.put(EventType.EXT_WRITE_ROWS, new CustomWriteRowsDeserializer(tableMap).setMayContainExtraInformation(true))
    eventMap.put(EventType.WRITE_ROWS, new CustomWriteRowsDeserializer(tableMap))
    val eventDeserializer = new EventDeserializer(
      new EventHeaderV4Deserializer, new NullEventDataDeserializer, eventMap, tableMap)
    client.setEventDeserializer(eventDeserializer)
    client
  }

  /** Creates an Akka stream Source for the given connection.
    *
    * @param params     connection parameters
    * @param bufferSize maximum number of records to keep in memory until they are requested.
    *                   when the buffer is full, back pressure is applied towards the MySQL
    *                   connection.
    */
  def asSource(params: ConnectionParams, bufferSize: Int = 1000): Source[RepoInsertEvent, NotUsed] = {
    val client = createClient(params)
    val q = new ArrayBlockingQueue[RepoInsertEvent](bufferSize)
    client.registerEventListener(new RepoInsertListener(q.put))

    val source: Source[RepoInsertEvent, NotUsed] = Source.unfoldResource(
      create = () => (),
      read = (_: Unit) => {
        // Blocks until something is available in our queue.
        Some(q.take())
      },
      close = (_: Unit) => {
        // when the stream is done, disconnects the client. This causes the thread to sleep.
        client.disconnect()
      }
    )

    new Thread("repo-binlog-event-loop") {
      override def run(): Unit = {
        client.connect()
      }
    }.start()

    source
  }
}
