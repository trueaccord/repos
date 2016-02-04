package repos

import org.reactivestreams.{Publisher, Subscriber, Subscription}
import repos.Action.{AggregationFunction, _}
import repos.SecondaryIndexQueries._

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import scala.util.{Failure, Success}

class InMemDb extends Container {

  private class InnerRepo[Id, M](repo: Repo[Id, M]) {
    private var pk = 1
    private val main = mutable.ArrayBuffer.empty[EntryTableRecord[Id, M]]
    private val latest = scala.collection.mutable.Map.empty[Id, (Long, M)]

    implicit def idOrdering: Ordering[Id] = Ordering.by(repo.idMapper.toUUID)

    private[repos] val indexMap: Map[String, InnerIndex[_]] = repo.allIndexes.map { i =>
      i.name -> new InnerIndex(i)
    }.toMap

    def insert(pairs: (Id, M)*) = {
      val entries: Seq[EntryTableRecord[Id, M]] = pairs.zipWithIndex.map {
        case ((id, m), index) => EntryTableRecord(pk + index, id, System.currentTimeMillis(), m)
      }
      main ++= entries
      val prepared: Seq[(Id, (Long, M))] = entries.map(e => (e.id ->(e.pk, e.entry)))
      latest ++= prepared
      indexMap.values.foreach(_.indexAction(prepared))
      pk += pairs.length
    }

    def get(id: Id) = latest.get(id).map(_._2)

    def delete(ids: Set[Id]) = {
      main --= main.filter(e => ids.contains(e.id))
      latest --= ids
    }

    def allLatestEntries: Seq[(Id, M)] = latest.map(t => (t._1, t._2._2)).toVector.sortBy(_._1)

    def lastEntry: Option[EntryTableRecord[Id, M]] = main.lastOption

    def getEntries(fromPk: Long, idsConstraint: Option[Seq[Id]], excludePks: Set[Long],
                   afterTimeMsec: Option[Long]): Seq[EntryTableRecord[Id, M]] = {
      main.view
        .filter(_.pk > fromPk)
        .applyIfDefined(idsConstraint)(q => idSet => q.filter(e => idSet.contains(e.id)))
        .applyIfDefined(afterTimeMsec)(q => timeMsec => q.filter(e => e.timestamp > timeMsec))
        .filterNot(e => excludePks.contains(e.pk))
        .toVector
    }

    class InnerIndex[R](index: SecondaryIndex[Id, M, R]) {
      val data = new mutable.HashMap[R, mutable.Set[(Long, Id)]] with mutable.MultiMap[R, (Long, Id)]
      implicit def rOrdering: Ordering[R] = index.projectionType.ordering

      def indexAction(entries: Iterable[(Id, (Long, M))]) = {
        for {
          (id, (pk, item)) <- entries
          r <- index.projection(item)
        } {
          data.addBinding(r, (pk, id))
        }
      }

      def find(criteria: LookupCriteria[R], offset: Option[Int], count: Option[Int]): Seq[(Id, M)] = {
        val t = (for {
          value <- data.keys.filter(criteriaToCollectionFilter(criteria)).toSeq.sorted
          (pk, id) <- data.getOrElse(value, Seq.empty).toVector.sortBy(i => repo.idMapper.toUUID(i._2))
          l <- latest.get(id).filter(_._1 == pk)
        } yield (id -> l._2)).toVector
          .applyIfDefined(offset)(_.drop)
          .applyIfDefined(count)(_.take)
        t.toVector
      }

      def count(c: LookupCriteria[R])(implicit ec: ExecutionContext) = find(c, None, None).size

      def criteriaToCollectionFilter(criteria: LookupCriteria[R]): (R => Boolean) = criteria match {
        case Equals(v) => _ == v
        case InSet(vs) => vs.contains
        case e@LargerThan(v) => e.ev.gt(_, v)
        case e@LargerThanOrEqual(v) => e.ev.gteq(_, v)
        case e@SmallerThan(v) => e.ev.lt(_, v)
        case e@SmallerThanOrEqual(v) => e.ev.lteq(_, v)
        case e@InRange(min, max) => t => e.ev.gteq(t, min) && e.ev.lt(t, max)
        case StartsWith(prefix) => { t: String => t.startsWith(prefix) }.asInstanceOf[R => Boolean]
      }

      private def allIndexedValues =
        (for {
          m <- data
          vpair <- m._2
          v <- latest if (v._1 == vpair._2 && v._2._1 == vpair._1)
        } yield m._1)

      def aggregate(agg: AggregationFunction) = agg match {
        case Max => Some(allIndexedValues.max)
        case Min => Some(allIndexedValues.min)
      }
    }
  }

  private val m = collection.mutable.Map.empty[String, InnerRepo[_, _]]

  private def innerRepo[Id, M, R](repo: Repo[Id, M]) = m.synchronized {
    m(repo.name).asInstanceOf[InnerRepo[repo.KeyType, repo.ValueType]]
  }

  private def withInnerRepo[Id, M, R](repo: Repo[Id, M])(block: InnerRepo[repo.KeyType, repo.ValueType] => R) = {
    val inner = innerRepo(repo)
    inner.synchronized {
      block(inner)
    }
  }

  private def innerIndex[Id, M, R](index: SecondaryIndex[Id, M, _]) = {
    val repoImpl = m(index.repo.name)
    repoImpl.indexMap(index.name).asInstanceOf[repoImpl.InnerIndex[R]]
  }

  private def withInnerIndex[Id, M, R, T](index: SecondaryIndex[Id, M, R])(b: InnerRepo[Id, M]#InnerIndex[R] => T): T = {
    withInnerRepo(index.repo) {
      repoImpl =>
        b(repoImpl.indexMap(index.name).asInstanceOf[repoImpl.InnerIndex[R]])
    }
  }

  def runRepoAction[S <: NoStream, Id, M, R](a: RepoAction[S, Id, M, R], ctx: Context)(implicit ec: ExecutionContext): Future[R] = {
    a match {
      case CreateAction(repo) =>
        m(repo.name) = new InnerRepo[repo.KeyType, repo.ValueType](repo)
        Future.successful(())
      case InsertAction(repo, entries) =>
        withInnerRepo(repo)(_.insert(entries: _*))
        Future.successful(())
      case GetAction(repo, id) =>
        withInnerRepo(repo)(_.get(id) match {
          case Some(o) => Future.successful(o.asInstanceOf[R])
          case None => Future.failed(new ElementNotFoundException(repo.idMapper.toUUID(id).toString))
        })
      case MultiGetAction(repo, ids) =>
        val result = withInnerRepo(repo) {
          r =>
            ids
              .map(i => (i, r.get(i))).collect {
              case (id, Some(obj)) => (id, obj)
            }
            .toMap.asInstanceOf[R]
        }
        Future.successful(result)
      case DeleteAction(repo, ids) =>
        withInnerRepo(repo)(_.delete(ids))
        Future.successful(())
      case GetLastEntry(repo) =>
        Future.successful(withInnerRepo(repo)(_.lastEntry))
      case GetEntriesAction(repo, fromPk, idsConstraint, excludePks, afterTimeMsec) =>
        runOrStream(ctx, Future.successful(withInnerRepo(repo)(_.getEntries(
          fromPk = fromPk, idsConstraint = idsConstraint, excludePks = excludePks,
          afterTimeMsec = afterTimeMsec))))
      case GetAllLatestEntriesAction(repo) =>
        runOrStream(ctx, Future.successful(withInnerRepo(repo)(_.allLatestEntries)))
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
  def createRepos(repo: Seq[Repo[_, _]]): Unit = m.synchronized {
    repo.foreach {
      repo =>
        m(repo.name) = new InnerRepo(repo)
    }
  }

  private var scannerCheckpoint: Map[(String, String), Long] = Map.empty[(String, String), Long]

  /** Finds the last pk the scanner has checkpointed for (jobName, repo) */
  def scannerFindLastPk[Id, M](jobName: String, repo: Repo[Id, M]): Future[Option[Long]] =
    Future.successful(scannerCheckpoint.get((jobName, repo.name)))

  /** Update the last pk the scanner has reached. */
  def scannerUpdateCheckpoint[Id, M](jobName: String, repo: Repo[Id, M], lastPk: Long): Future[Unit] = {
    scannerCheckpoint = scannerCheckpoint + ((jobName, repo.name) -> lastPk)
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
