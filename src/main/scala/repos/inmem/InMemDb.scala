package repos.inmem

import org.reactivestreams.{Publisher, Subscriber, Subscription}
import repos.Action._
import repos._

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import scala.util.{Failure, Success}

class InMemDb extends Database {
  private val repoMap: collection.concurrent.Map[String, InMemRepoImpl[_, _]] =
    collection.concurrent.TrieMap.empty

  private def withRepoImpl[Id, M, R](repo: Repo[Id, M])(block: InMemRepoImpl[repo.KeyType, repo.ValueType] => R) = {
    val inner = repoMap(repo.name).asInstanceOf[InMemRepoImpl[repo.KeyType, repo.ValueType]]
    inner.synchronized {
      block(inner)
    }
  }

  private def withInnerIndex[Id, M, R, T](index: SecondaryIndex[Id, M, R])(b: InMemRepoImpl[Id, M]#InnerIndex[R] => T): T = {
    withRepoImpl(index.repo) {
      repoImpl =>
        b(repoImpl.indexMap(index.name).asInstanceOf[repoImpl.InnerIndex[R]])
    }
  }

  def runRepoAction[S <: NoStream, Id, M, R](a: RepoAction[S, Id, M, R], ctx: Context)(implicit ec: ExecutionContext): Future[R] = {
    a match {
      case CreateAction(repo) =>
        repoMap(repo.name) = new InMemRepoImpl[repo.KeyType, repo.ValueType](repo)
        Future.successful(())
      case InsertAction(repo, entries) =>
        withRepoImpl(repo)(_.insert(entries: _*))
        Future.successful(())
      case GetAction(repo, id) =>
        withRepoImpl(repo)(_.get(id) match {
          case Some(o) => Future.successful(o.asInstanceOf[R])
          case None => Future.failed(new ElementNotFoundException(repo.idMapper.toUUID(id).toString))
        })
      case MultiGetAction(repo, ids) =>
        val result = withRepoImpl(repo) {
          r =>
            ids
              .map(i => (i, r.get(i))).collect {
              case (id, Some(obj)) => (id, obj)
            }
            .toMap.asInstanceOf[R]
        }
        Future.successful(result)
      case DeleteAction(repo, ids) =>
        withRepoImpl(repo)(_.delete(ids))
        Future.successful(())
      case GetLastEntry(repo) =>
        Future.successful(withRepoImpl(repo)(_.lastEntry))
      case GetEntriesAction(repo, fromPk, idsConstraint, excludePks, afterTimeMsec, count) =>
        runOrStream(ctx, Future.successful(withRepoImpl(repo)(_.getEntries(
          fromPk = fromPk, idsConstraint = idsConstraint, excludePks = excludePks,
          afterTimeMsec = afterTimeMsec, count = count))))
      case GetAllLatestEntriesAction(repo) =>
        runOrStream(ctx, Future.successful(withRepoImpl(repo)(_.allLatestEntries)))
      case IndexGetAllAction(index, criteria, offset, count) =>
        runOrStream(ctx, Future.successful(withInnerIndex(index)(_.find(criteria, offset, count))))
      case IndexAggegrationAction(index, agg) =>
        Future.successful(withInnerIndex(index)(_.aggregate(agg)))
      case IndexCountAction(index, value) =>
        Future.successful(withInnerIndex(index)(_.count(value)))
    }
  }

  private def runOrStream[T](ctx: Context, data: Future[Seq[T]])(implicit ec: ExecutionContext) = Context.go(ctx)(data)(new Publisher[T] {

    override def subscribe(s: Subscriber[_ >: T]): Unit = {
      data.onComplete {
        case Success(d) => s.onSubscribe(new InMemDb.InMemorySubscription[T](s, d))
        case Failure(th) => s.onError(th)
      }
    }
  })

  // Test utils
  def createRepos(repo: Seq[Repo[_, _]]): Unit = repoMap.synchronized {
    repo.foreach {
      repo =>
        repoMap(repo.name) = new InMemRepoImpl(repo)
    }
    scannerCheckpoint.clear()
  }

  private val scannerCheckpoint: collection.concurrent.Map[(String, String), Long] =
    collection.concurrent.TrieMap.empty

  /** Finds the last pk the scanner has checkpointed for (jobName, repo) */
  def scannerFindLastPk[Id, M](jobName: String, repo: Repo[Id, M]): Future[Option[Long]] =
    Future.successful(scannerCheckpoint.get((jobName, repo.name)))

  /** Update the last pk the scanner has reached. */
  def scannerUpdateCheckpoint[Id, M](jobName: String, repo: Repo[Id, M], lastPk: Long): Future[Unit] = {
    scannerCheckpoint += ((jobName, repo.name) -> lastPk)
    Future.successful(())
  }
}


object InMemDb {

  private[repos] class InMemorySubscription[T](subscriber: Subscriber[_ >: T], data: Seq[T]) extends Subscription {
    @volatile var pos = 0

    override def cancel(): Unit = {}

    override def request(n: Long): Unit = {
      require(n > 0)
      var i = 0
      while (i < n && pos < data.size) {
        val t = data(pos)
        i += 1
        pos += 1
        subscriber.onNext(t)
      }
      if (pos == data.size) {
        subscriber.onComplete()
      }
    }
  }
}
