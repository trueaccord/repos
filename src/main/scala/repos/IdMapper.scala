package repos

import java.util.UUID

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context

trait IdType extends Any {
  def untyped: UUID
}

/** IdMapper providers implicits that help manipulating Id types in a generic way with slick.
  *
  * @tparam T the type of Id.
  */

trait IdMapper[T] {
  def fromUUID(untyped: UUID): T

  def toUUID(t: T): UUID
}

/** We define a macro that implicitly creates IdFactories when they are needed.
  * Doing this eliminates the need to repetitively write an IdFactory for each case class.
  * Also, in many source files we import all types with a wildcard, this prevents importing
  * many implicit IdFactories into the scope.
  */
object IdMapper {
  implicit def materializeIdFactory[T <: IdType]: IdMapper[T] = macro materializeIdFactoryImpl[T]

  def materializeIdFactoryImpl[T: c.WeakTypeTag](c: Context): c.Expr[IdMapper[T]] = {
    import c.universe._
    val tpe = weakTypeOf[T]
    val companion = tpe.typeSymbol.companion

    c.Expr[IdMapper[T]] {
      q"""
      new _root_.repos.IdMapper[$tpe] {
        def fromUUID(s: java.util.UUID): $tpe = $companion(s)
        def toUUID(t: $tpe): java.util.UUID = t.untyped
      }
    """
    }
  }

  private val ZeroUUID: UUID = new UUID(0, 0)

  private val IdRegex = "[0-9a-f]{32}".r.pattern

  def isValidId(s: String) = s.isEmpty || IdRegex.matcher(s).matches()
}

