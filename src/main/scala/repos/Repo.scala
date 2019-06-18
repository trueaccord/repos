package repos

import org.reactivestreams.{Subscription, Subscriber, Publisher}
import repos.SecondaryIndex._

import scala.language.implicitConversions

import scala.concurrent.{Promise, Future, ExecutionContext}
import scala.language.higherKinds
import scala.language.existentials
import scala.reflect.ClassTag
import Action._

import scala.util.{Failure, Success}

class Repo[Id, M](val name: String)(implicit val idMapper: IdMapper[Id], val dataMapper: DataMapper[M], val idClass: ClassTag[Id], val mClass: ClassTag[M]) extends IndexTableMethods[Id, M] {
  self =>
  type KeyType = Id
  type ValueType = M
  type IndexesList = List[SecondaryIndex[Id, M, R]] forSome {type R}

  def create(): CreateAction[Id, M] = CreateAction(this)

  def insert(id: Id, m: M): InsertAction[Id, M] = insert((id, m))

  def insert(entries: (Id, M)*) = InsertAction(this, entries)

  def apply(id: Id): GetAction[Id, M] = GetAction(this, id)

  def multiGet(ids: Iterable[Id]): MultiGetAction[Id, M] = MultiGetAction(this, ids)

  def delete(ids: Set[Id]): DeleteAction[Id, M] = DeleteAction(this, ids)

  def getEntries(fromPk: Long = -1, idsConstraint: Option[Seq[Id]] = None,
                 excludePks: Set[Long] = Set.empty, afterTimeMsec: Option[Long] = None,
                 count: Option[Int] = None): GetEntriesAction[Id, M] =
    GetEntriesAction(repo = this, fromPk = fromPk, idsConstraint = idsConstraint,
      excludePks = excludePks, afterTimeMsec = afterTimeMsec, count = count)

  def lastEntry() = GetLastEntry(this)

  def allLatestEntries(): GetAllLatestEntriesAction[Id, M] = GetAllLatestEntriesAction(this)

  def allIndexes: IndexesList = (for {
    m <- this.getClass().getMethods.view
    if m.getReturnType == classOf[SecondaryIndex[_, _, _]] && m.getParameterTypes.isEmpty
  } yield m.invoke(this)).toList.asInstanceOf[IndexesList]
}

trait IndexTableMethods[Id, M] {
  this: Repo[Id, M] =>
  def multiIndexTable[R1 : ProjectionType](name: String)(f: M => Seq[R1]) = SecondaryIndex[Id, M, R1](this, name, f)

  def indexTable[R1 : ProjectionType](name: String)(f: M => R1) = multiIndexTable[R1](name) { m: M => Seq(f(m)) }

  def partialIndexTable[R1 : ProjectionType](name: String)(f: PartialFunction[M, R1]) = multiIndexTable[R1](name)(f.lift.andThen(_.toSeq))
}

abstract class RepoPublisher[T] extends Publisher[T] {
  /** Consume the stream, processing each element sequentially on the specified ExecutionContext.
    * The resulting Future completes when all elements have been processed or an error was
    * signaled. */
  def foreach[U](f: T => U)(implicit ec: ExecutionContext): Future[Unit] = {
    val p = Promise[Unit]()
    @volatile var lastMsg: Future[Any] = null
    @volatile var subscr: Subscription = null
    subscribe(new Subscriber[T] {
      def onSubscribe(s: Subscription): Unit = {
        subscr = s
        s.request(1L)
      }
      def onComplete(): Unit = {
        val l = lastMsg
        if(l ne null) l.onComplete {
          case Success(_) => p.trySuccess(())
          case Failure(t) => p.tryFailure(t)
        }
        else p.trySuccess(())
      }
      def onError(t: Throwable): Unit = {
        val l = lastMsg
        if(l ne null) l.onComplete(_ => p.tryFailure(t))
        else p.tryFailure(t)
      }
      def onNext(t: T): Unit = {
        lastMsg = Future(f(t))
        lastMsg.onComplete {
          case Success(v) =>
            subscr.request(1L)
          case Failure(t) =>
            subscr.cancel()
            p.tryFailure(t)
        }
      }
    })
    p.future
  }
}

