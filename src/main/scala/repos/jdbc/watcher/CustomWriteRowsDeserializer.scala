package repos.jdbc.watcher

import java.util

import com.github.shyiko.mysql.binlog.event.TableMapEventData
import com.github.shyiko.mysql.binlog.event.deserialization.WriteRowsEventDataDeserializer
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream

/** Due to https://github.com/shyiko/mysql-binlog-connector-java/issues/112 */
class CustomWriteRowsDeserializer(tableMapEventData: util.Map[java.lang.Long, TableMapEventData])
  extends WriteRowsEventDataDeserializer(tableMapEventData) {

  override def deserializeString(length: Int, inputStream: ByteArrayInputStream): Array[Byte] = {
    val stringLength = if (length < 256) inputStream.readInteger(1) else inputStream.readInteger(2)
    inputStream.read(stringLength)
  }
}
