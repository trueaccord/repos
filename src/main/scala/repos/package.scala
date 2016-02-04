package object repos {

  implicit class ApplyIfDefined[M](val obj: M) extends AnyVal {
    /** Syntactic sugar to apply a function on an object only if an optional value is defined.
      *
      * If the optional value is None, the object returned as is, otherwise `f` is passed
      * the value of the object and the option:
      *
      * Example:
      *
      * You want to limit a sequence to an optional number of items:
      *
      * myItems.filterIfDefined(optionalCount)(_.take)
      *
      * The above is equivalent to:
      *
      * myItems.filterIfDefined(optionalCount)(obj => count => obj.take(count))
      */
    def applyIfDefined[V, T <: M](opt: Option[V])(f: M => V => T): M = opt match {
      case Some(v) => f(obj)(v)
      case None => obj
    }
  }
}
