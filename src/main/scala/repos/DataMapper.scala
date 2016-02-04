package repos

trait DataMapper[M] {
  def toBytes(m: M): Array[Byte]

  def fromBytes(b: Array[Byte]): M
}
