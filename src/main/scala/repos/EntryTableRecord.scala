package repos

case class EntryTableRecord[Id, M](pk: Long, id: Id, timestamp: Long, entry: M)
