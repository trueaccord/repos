package repos

import Action._
import repos.SecondaryIndexQueries.LookupCriteria

class ElementNotFoundException(id: String) extends RepoException(s"Key '$id' could be found")

sealed trait Action[+S <: NoStream, +A] {
  def zip[B](v: Action[NoStream, B]): Action[NoStream, (A, B)] = Tuple2Action(this, v)

  def andThen[S2 <: NoStream, B](v: Action[S2, B]): Action[S2, B] = v match {
    case AndThenAction(is) => AndThenAction[S2, B](this +: is)
    case _ => AndThenAction(Vector(this, v))
  }

  def flatMap[S2 <: NoStream, B](f: A => Action[S2, B]): Action[S2, B] = FlatMapAction[S2, A, B](this, f)

  def map[B](f: A => B): Action[NoStream, B] = MappedAction(this, f)
}

object Action {
  def seq[S <: NoStream, A](actions: Seq[Action[S, A]]): Action[NoStream, Seq[A]] = ActionSeq[A](actions)

  case class AndThenAction[+S <: NoStream, B](indexedSeq: IndexedSeq[Action[NoStream, Any]]) extends Action[S, B]

  case class Tuple2Action[+S <: NoStream, A, B](a: Action[S, A], b: Action[S, B]) extends Action[NoStream, (A, B)]

  case class FlatMapAction[+S <: NoStream, A, +B](source: Action[NoStream, A], f: A => Action[S, B]) extends Action[S, B]

  case class MappedAction[S <: NoStream, A, B](source: Action[S, A], f: A => B) extends Action[NoStream, B]

  case class ActionSeq[A](actions: Seq[Action[NoStream, A]]) extends Action[NoStream, Seq[A]]

//  case class MapStreamAction[K, L, A](source: Action[Streaming[K], A], f: K => L) extends Action[Streaming[L], A]

  class RepoException(msg: String) extends Exception

  trait RepoAction[+S <: NoStream, Id, M, A] extends Action[S, A] {
    type RepoType = Repo[Id, M]
    def repo: RepoType
  }

  case class InsertAction[Id, M](repo: Repo[Id, M], entries: Seq[(Id, M)], insertIntoLatest: Boolean = true) extends RepoAction[NoStream, Id, M, Unit]

  case class GetAction[Id, M](repo: Repo[Id, M], id: Id) extends RepoAction[NoStream, Id, M, M]

  case class MultiGetAction[Id, M](repo: Repo[Id, M], ids: Iterable[Id]) extends RepoAction[NoStream, Id, M, Map[Id, M]]

  case class GetEntriesAction[Id, M](repo: Repo[Id, M], fromPk: Long, idsConstraint: Option[Seq[Id]],
                                     excludePks: Set[Long], afterTimeMsec: Option[Long],
                                     count: Option[Int] = None) extends RepoAction[Streaming[EntryTableRecord[Id, M]], Id, M, Seq[EntryTableRecord[Id, M]]]

  case class GetLastEntry[Id, M](repo: Repo[Id, M]) extends RepoAction[NoStream, Id, M, Option[EntryTableRecord[Id, M]]]

  case class GetAllLatestEntriesAction[Id, M](repo: Repo[Id, M]) extends RepoAction[Streaming[(Id, M)], Id, M, Seq[(Id, M)]]

  case class DeleteAction[Id, M](repo: Repo[Id, M], ids: Set[Id]) extends RepoAction[NoStream, Id, M, Unit]

  case class CreateAction[Id, M](repo: Repo[Id, M]) extends RepoAction[NoStream, Id, M, Unit]

  trait IndexAction[Id, M, R, A] extends RepoAction[NoStream, Id, M, A] {
    def index: SecondaryIndex[Id, M, R]
    override def repo = index.repo
  }

  trait IndexStreamingAction[Id, M, R, A] extends RepoAction[Streaming[(Id, M)], Id, M, A] {
    def index: SecondaryIndex[Id, M, R]
    override def repo = index.repo
  }

  case class IndexGetAllAction[Id, M, R]
  (index: SecondaryIndex[Id, M, R], criteria: LookupCriteria[R], offset: Option[Int] = None, count: Option[Int] = None) extends IndexStreamingAction[Id, M, R, Seq[(Id, M)]]

  case class IndexCountAction[Id, M, R](index: SecondaryIndex[Id, M, R], criteria: LookupCriteria[R]) extends IndexAction[Id, M, R, Int]

  case class IndexAggegrationAction[Id, M, R](index: SecondaryIndex[Id, M, R], agg: AggregationFunction) extends IndexAction[Id, M, R, Option[R]]

  sealed trait AggregationFunction

  case object Max extends AggregationFunction

  case object Min extends AggregationFunction

  implicit class StreamAction[K, A](val a: Action[Streaming[K], A]) extends AnyVal {
//    def mapStream[L](f: K => L): Action[Streaming[L], A] = MapStreamAction[K, L, A](a, f)
  }
}
