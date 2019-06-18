package repos

import java.util.UUID

import repos.SecondaryIndex.ProjectionType
import scala.language.existentials
import scala.language.implicitConversions
import scala.reflect.ClassTag

case class SecondaryIndex[Id, M, R](repo: Repo[Id, M], name: String, projection: M => Seq[R])(implicit val projectionType : ProjectionType[R])

class SecondaryIndexQueries[Id, M, R](val index: SecondaryIndex[Id, M, R]) extends AnyVal {
  import SecondaryIndexQueries._
  import Action._

  type LookupFunction = ExpectLookupCriteria[R] => LookupCriteria[R]

  def find(m: LookupFunction,
           offset: Option[Int] = None,
           count: Option[Int] = None) =
    IndexGetAllAction(index, m(ExpectLookupCriteria(index)), offset = offset, count = count)

  def count(m: LookupFunction) =
    IndexCountAction(index, m(ExpectLookupCriteria(index)))

  def allMatching(v: R, offset: Option[Int] = None, count: Option[Int] = None) =
    IndexGetAllAction(index, Equals(v), offset, count)

  def countMatching(v: R): Action[NoStream, Int] = IndexCountAction(index, Equals(v))

  def first(m: LookupFunction): Action[NoStream, Option[(Id, M)]] = find(m, count = Some(1)).map(_.headOption)

  def max: Action[NoStream, Option[R]] = IndexAggegrationAction(index, Max)

  def min: Action[NoStream, Option[R]] = IndexAggegrationAction(index, Min)
}

object SecondaryIndexQueries {
  sealed trait LookupCriteria[R]

  case class Equals[R](v: R) extends LookupCriteria[R]

  case class InSet[R](vs: Set[R]) extends LookupCriteria[R]

  case class LargerThan[R](v: R)(implicit val ev: Ordering[R]) extends LookupCriteria[R]

  case class LargerThanOrEqual[R](v: R)(implicit val ev: Ordering[R]) extends LookupCriteria[R]

  case class SmallerThan[R](v: R)(implicit val ev: Ordering[R]) extends LookupCriteria[R]

  case class SmallerThanOrEqual[R](v: R)(implicit val ev: Ordering[R]) extends LookupCriteria[R]

  case class InRange[R](min: R, max: R)(implicit val ev: Ordering[R]) extends LookupCriteria[R]

  case class StartsWith[R](prefix: String)(implicit ev: R =:= String) extends LookupCriteria[R]

  case class ExpectLookupCriteria[R](index: SecondaryIndex[_, _, R]) {
    def matching(v: R): Equals[R] = Equals(v)

    def inSet(vs: Set[R]): InSet[R] = InSet(vs)

    def largerThan(v: R)(implicit o: Ordering[R]): LargerThan[R] = LargerThan(v)

    def largerThanOrEqual(v: R)(implicit o: Ordering[R]): LargerThanOrEqual[R] = LargerThanOrEqual(v)

    def smallerThan(v: R)(implicit o: Ordering[R]): SmallerThan[R] = SmallerThan(v)

    def smallerThanOrEqual(v: R)(implicit o: Ordering[R]): SmallerThanOrEqual[R] = SmallerThanOrEqual(v)

    def inRange(start: R, end: R)(implicit o: Ordering[R]): InRange[R] = InRange(start, end)
  }

  object ExpectLookupCriteria {
    implicit class StringExtensions(val secondaryIndex: ExpectLookupCriteria[String]) extends AnyVal {
      def startsWith(prefix: String) = StartsWith(prefix)
    }
  }
}

object SecondaryIndex {
  implicit def toQueries[Id, M, R](v: SecondaryIndex[Id, M, R]): SecondaryIndexQueries[Id, M, R] =
    new SecondaryIndexQueries(v)

  sealed abstract class ProjectionType[A](implicit val ordering : Ordering[A]) {
    type ScalaType = A
  }

  object ProjectionType {
    implicit object BooleanProjection extends ProjectionType[Boolean]

    implicit object CharProjection extends ProjectionType[Char]

    implicit object IntProjection extends ProjectionType[Int]

    implicit object LongProjection extends ProjectionType[Long]

    implicit object StringProjection extends ProjectionType[String]

    implicit object UuidProjection extends ProjectionType[UUID]

    class MappedProjection[T, A](val from: T => A, val to: A => T)(implicit val base: ProjectionType[T], val classTag: ClassTag[A]) extends ProjectionType[A]()(Ordering.by(to)(base.ordering))

    def mapped[T : ProjectionType, A : ClassTag](from: T => A)(to: A => T): MappedProjection[T, A] =
      new MappedProjection[T, A](from, to)
  }

}
