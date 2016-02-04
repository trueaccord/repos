package repos

sealed trait NoStream

trait Streaming[+A] extends NoStream
