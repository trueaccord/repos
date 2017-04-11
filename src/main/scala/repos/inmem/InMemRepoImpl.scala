package repos.inmem

import repos.Action.{Min, Max, AggregationFunction}
import repos.SecondaryIndexQueries._
import repos.{SecondaryIndex, EntryTableRecord, Repo}
import repos.ApplyIfDefined

import scala.collection.mutable
import scala.concurrent.ExecutionContext

private class InMemRepoImpl[Id, M](repo: Repo[Id, M]) {
  private var pk = 1
  private val main = mutable.ArrayBuffer.empty[EntryTableRecord[Id, M]]
  private val latest = scala.collection.mutable.Map.empty[Id, (Long, M)]

  implicit def idOrdering: Ordering[Id] = Ordering.by(repo.idMapper.toUUID)

  private[repos] val indexMap: Map[String, InnerIndex[_]] = repo.allIndexes.map { i =>
    i.name -> new InnerIndex(i)
  }.toMap

  def insert(pairs: (Id, M)*): Unit = {
    val prepared = insertWithoutLatest(pairs: _*)
    latest ++= prepared
  }

  def insertWithoutLatest(pairs: (Id, M)*): Seq[(Id, (Long, M))] = {
    val entries: Seq[EntryTableRecord[Id, M]] = pairs.zipWithIndex.map {
      case ((id, m), index) => EntryTableRecord(pk + index, id, System.currentTimeMillis(), m)
    }
    main ++= entries
    val prepared: Seq[(Id, (Long, M))] = entries.map(e => e.id -> (e.pk, e.entry))
    indexMap.values.foreach(_.indexAction(prepared))
    pk += pairs.length
    prepared
  }

  def get(id: Id) = latest.get(id).map(_._2)

  def delete(ids: Set[Id]) = {
    main --= main.filter(e => ids.contains(e.id))
    latest --= ids
  }

  def allLatestEntries: Seq[(Id, M)] = latest.map(t => (t._1, t._2._2)).toVector.sortBy(_._1)

  def lastEntry: Option[EntryTableRecord[Id, M]] = main.lastOption

  def getEntries(fromPk: Long, idsConstraint: Option[Seq[Id]], excludePks: Set[Long],
                 afterTimeMsec: Option[Long], count: Option[Int] = None): Seq[EntryTableRecord[Id, M]] = {
    main.view
      .filter(_.pk > fromPk)
      .applyIfDefined(idsConstraint)(q => idSet => q.filter(e => idSet.contains(e.id)))
      .applyIfDefined(afterTimeMsec)(q => timeMsec => q.filter(e => e.timestamp > timeMsec))
      .filterNot(e => excludePks.contains(e.pk))
      .applyIfDefined(count)(_.take)
      .toVector
  }

  class InnerIndex[R](index: SecondaryIndex[Id, M, R]) {
    val data = new mutable.HashMap[R, mutable.Set[(Long, Id)]] with mutable.MultiMap[R, (Long, Id)]
    implicit def rOrdering: Ordering[R] = index.projectionType.ordering

    def indexAction(entries: Iterable[(Id, (Long, M))]) = {
      for {
        (id, (pk, item)) <- entries
        r <- index.projection(item).distinct
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
