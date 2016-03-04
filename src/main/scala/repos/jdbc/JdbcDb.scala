package repos.jdbc

import java.util.UUID

import org.apache.commons.codec.digest.DigestUtils
import org.xerial.snappy.Snappy
import repos.Action._
import repos.SecondaryIndex._
import repos.SecondaryIndexQueries._
import repos._
import slick.dbio.Effect.Read
import slick.driver.JdbcProfile
import slick.lifted.CanBeQueryCondition
import slick.profile.FixedSqlStreamingAction

import scala.concurrent.{ExecutionContext, Future}
import scala.language.{existentials, higherKinds}
import scala.reflect.ClassTag


class JdbcDb(val profile: JdbcProfile, private[repos] val db: JdbcProfile#Backend#Database) extends Database {

  import profile.api._

  private implicit def columnMapper[Id: ClassTag](implicit idMapper: IdMapper[Id]): BaseColumnType[Id] =
    MappedColumnType.base[Id, UUID](idMapper.toUUID, idMapper.fromUUID)

  private def streamOrRun[R, T, E <: slick.dbio.Effect](ctx: Context)(q: FixedSqlStreamingAction[R, T, E]) = Context.go(ctx)(db.run(q))(db.stream(q))

  class RepoImpl[Id: ClassTag, M: ClassTag : DataMapper](repo: Repo[Id, M])(implicit idMapper: IdMapper[Id]) {
    private[repos] val latestTableName = repo.name + "_latest"
    private[repos] val entryTable = new TableQuery(new EntryTable[Id, M](_, tableName = repo.name))
    private[repos] val latestEntryTable = new TableQuery(new LatestEntryTable[Id, M](_, latestTableName))
    private[repos] val indexMap: Map[String, IndexTableImpl[_]] = repo.allIndexes.map { i =>
      i.name -> new IndexTableImpl(i)(i.projectionType)
    }.toMap

    def insert(elements: Iterable[(Id, M)], skipIndexesForTesting: Boolean = false)(implicit ec: ExecutionContext): Future[Seq[Long]] = {
      val newEntries = elements.map(e => EntryTableRecord[Id, M](-1, e._1, System.currentTimeMillis(), e._2))

      val effectiveIndexMap =
        if (skipIndexesForTesting) Map.empty[String, IndexTableImpl[_]] else
        indexMap
      // Build the insert as a single DBAction, but not a transaction.
      // Transactions in Slick 3 make it more challenging to tune the maximum number of connections:
      // Since between the steps there are computations (flatMaps) that require computations
      // between the actions, the Slick thread get released and uses a new connection. The connect
      // that is allocated to the transaction is not owned by any Slick thread until the transaction
      // completes. This makes it possible for numConnections > numSlickThreads which may sometimes
      // exceed the number of connections in the pool.
      val r = for {
        // log table
        pkList <- entryTable.returning(entryTable.map(_.pk)) ++= newEntries

        // latest table
        _ <- DBIO.sequence((newEntries zip pkList).toSeq.sortBy(t => idMapper.toUUID(t._1.id)).map {
          e => latestEntryTable.insertOrUpdate((e._1.id, e._1.entry, e._2))
        })

        // indexes
        _ <- DBIO.sequence(effectiveIndexMap.values.map(
          _.buildInsertAction(elements zip pkList)))
      } yield pkList
      db.run(r)
    }

    private val getLatestByIdQuery = latestEntryTable.findBy(_.id)

    def get(id: Id)(implicit ec: ExecutionContext): Future[Option[M]] =
      db.run(getLatestByIdQuery(id).result).map(_.headOption.map(_._2))

    def multiGet(ids: Iterable[Id])(implicit ec: ExecutionContext): Future[Map[Id, M]] = {
      val zs: Iterator[Future[Map[Id, M]]] = ids.grouped(2000).map {
        idGroup =>
          db.run(
            latestEntryTable.filter(_.id inSet idGroup)
              .map(e => (e.id, e.format, e.entryBin)).result)
            .map(_.map {
              kv => (kv._1, JdbcDb.parse(kv._2, kv._3))
            }.toMap)
      }
      Future.sequence(zs).map(_.foldLeft(Map.empty[Id, M])(_ ++ _))
    }

    def delete(ids: Set[Id])(implicit ec: ExecutionContext): Future[Unit] = {
      db.run(
        entryTable.filter(_.uuid inSet ids).delete andThen
          latestEntryTable.filter(_.id inSet ids).delete).map(_ => ())
    }

    def getEntries(fromPk: Long, idsConstraint: Option[Seq[Id]], excludePks: Set[Long],
                   afterTimeMsec: Option[Long], count: Option[Int] = None, ctx: Context): Future[Seq[EntryTableRecord[Id, M]]] = {
      val q0 = entryTable
        .filter(_.pk > fromPk)
        .applyIfDefined(idsConstraint)(q => idSet =>q.filter(_.uuid inSet idSet))
        .applyIfDefined(afterTimeMsec)(q => timeMsec => q.filter(_.timeMsec > afterTimeMsec))
      val q1 = if (excludePks.isEmpty) q0 else q0.filterNot(_.pk inSet excludePks)
      streamOrRun(ctx)(
        q1.sortBy(_.pk).applyIfDefined(count)(_.take).result)
    }

    def lastEntry()(implicit ec: ExecutionContext): Future[Option[EntryTableRecord[Id, M]]] = {
      db.run(entryTable.sortBy(_.pk.desc).take(1).result.map(_.headOption))
    }

    def allLatestEntries(ctx: Context): Future[Seq[(Id, M)]] = {
      streamOrRun(ctx)(latestEntryTable.map(t => (t.id, t.entry)).sortBy(_._1).result)
    }

    def create(implicit ec: ExecutionContext) = {
      val s = DBIO.sequence(Seq(
        entryTable.schema.create,
        latestEntryTable.schema.create) ++ indexMap.values.map(_.indexTable.schema.create))
      db.run(s).map(_ => ())
    }

    class IndexTableImpl[R](index: SecondaryIndex[Id, M, R])(implicit rp: ProjectionType[R]) {
      implicit def baseColumnType[R](implicit p: ProjectionType[R]): BaseColumnType[R] = p match {
        case ProjectionType.BooleanProjection => implicitly[BaseColumnType[Boolean]]
        case ProjectionType.IntProjection => implicitly[BaseColumnType[Int]]
        case ProjectionType.LongProjection => implicitly[BaseColumnType[Long]]
        case ProjectionType.UuidProjection => implicitly[BaseColumnType[UUID]]
        case ProjectionType.StringProjection => implicitly[BaseColumnType[String]]
        case ProjectionType.CharProjection => implicitly[BaseColumnType[Char]]
        case e: ProjectionType.MappedProjection[_, R] =>
          MappedColumnType.base(e.to, e.from)(e.classTag, baseColumnType(e.base))
      }

      def ix3TableName: String = s"ix3_${index.repo.name}__${index.name}"

      val indexTable = new TableQuery(new Ix3Table[Id, R](_, ix3TableName)(repo.idMapper, repo.idClass, baseColumnType(rp)))

      def buildInsertAction(entries: Iterable[((Id, M), Long)]) = {
        indexTable ++= (for {
          ((id, item), pk) <- entries
          r <- index.projection(item).distinct
        } yield (id, r, pk))
      }

      def allSatisfyingQuery[T <: Rep[_] : CanBeQueryCondition](sqlFilter: Rep[R] => T, offset: Option[Int] = None, count: Option[Int] = None): FixedSqlStreamingAction[Seq[(Id, M)], (Id, M), Read] = {
        // Ids emitted by this query are distinct: (id, parentPk, value) are unique in ix3Delegate
        // but even if we get (id, parentPk1, value) and (id, parentPk2, value) emitted from the first
        // query, there is at most one id with that parentPk in latestEntryTable.
        (for {
          m <- indexTable if sqlFilter(m.value)
          v <- latestEntryTable if (v.id === m.id && v.parentPk === m.parentPk)
        } yield (m.value, v.id, v.entry)).sortBy(x => (x._1, x._2)).map(x => (x._2, x._3))
          .applyIfDefined(offset)(obj => i => obj.drop(i))
          .applyIfDefined(count)(_.take).result
      }

      private def idsSatisfying[T <: Rep[_]](sqlFilter: Rep[R] => T)(implicit wt: CanBeQueryCondition[T]) =
        (for {
          m <- indexTable if sqlFilter(m.value)
          v <- latestEntryTable if (v.id === m.id && v.parentPk === m.parentPk)
        } yield v.id)

      private def allIndexedValues =
        (for {
          m <- indexTable
          v <- latestEntryTable if (v.id === m.id && v.parentPk === m.parentPk)
        } yield m.value)

      def criteriaToSqlFilter(criteria: LookupCriteria[R]): (Rep[R] => Rep[Boolean]) = criteria match {
        case Equals(v) => _ === v
        case InSet(vs) => _.inSet(vs)
        case LargerThan(v) => _ > v
        case LargerThanOrEqual(v) => _ >= v
        case SmallerThan(v) => _ < v
        case SmallerThanOrEqual(v) => _ <= v
        case InRange(min, max) => (t => t >= min && t < max)
        case StartsWith(prefix) => { t: Rep[String] => t.startsWith(prefix) }.asInstanceOf[Rep[R] => Rep[Boolean]]
      }

      def all(criteria: LookupCriteria[R], offset: Option[Int], count: Option[Int], ctx: Context)(implicit ec: ExecutionContext): Future[Seq[(Id, M)]] =
        streamOrRun(ctx)(allSatisfyingQuery(criteriaToSqlFilter(criteria), offset, count))

      def count(criteria: LookupCriteria[R]): Future[Int] = {
        db.run(idsSatisfying(criteriaToSqlFilter(criteria)).size.result)
      }

      def aggregate(agg: AggregationFunction): Future[Option[R]] = agg match {
        case Max => db.run(allIndexedValues.max.result)
        case Min => db.run(allIndexedValues.min.result)
      }
    }
  }

  private[repos] class EntryTable[Id: ClassTag, M](tag: Tag, tableName: String)(implicit idMapper: IdMapper[Id], dataMapper: DataMapper[M])
    extends Table[EntryTableRecord[Id, M]](tag, tableName) {
    def pk = column[Long]("pk", O.AutoInc, O.PrimaryKey)

    def uuid = column[Id]("uuid")(columnMapper[Id])

    def timeMsec = column[Long]("time_msec")

    def entryBin = column[Array[Byte]]("entry_bin", O.SqlType("longblob"))

    def format = column[Char]("format")

    override def * = (pk, uuid, timeMsec, format, entryBin) <>( {
      (pk: Long, uuid: Id, time: Long, format: Char, entryBin: Array[Byte]) =>
        EntryTableRecord[Id, M](pk, uuid, time, JdbcDb.parse(format, entryBin))
    }.tupled, {
      e: EntryTableRecord[Id, M] =>
        val (format, entryBin) = JdbcDb.serialize(e.entry)
        Some((e.pk, e.id, e.timestamp, format, entryBin))
    })

    def uuidPkIndex = index(s"${tableName}_uuid_pk_idx", (uuid, pk), unique = false)

    def timeMsecIndex = index(s"${tableName}_time_msec_idx", timeMsec, unique = false)
  }

  private[repos] class LatestEntryTable[Id: ClassTag, M: ClassTag](tag: Tag, latestEntryTable: String)(implicit idMapper: IdMapper[Id], dataMapper: DataMapper[M])
    extends Table[(Id, M, Long)](tag, latestEntryTable) {
    def id = column[Id]("id", O.PrimaryKey)(columnMapper[Id])

    def format = column[Char]("format")

    def entryBin = column[Array[Byte]]("entry_bin", O.SqlType("longblob"))

    def parentPk = column[Long]("parent_pk")

    def entry = (format, entryBin) <>( { z: (Char, Array[Byte]) => JdbcDb.parse[M](z._1, z._2) }, { m: M => Some(JdbcDb.serialize[M](m)) })

    override def * = (id, format, entryBin, parentPk) <>( {
      t: (Id, Char, Array[Byte], Long) => (t._1, JdbcDb.parse(t._2, t._3), t._4)
    }, {
      e: (Id, M, Long) =>
        val (format, entryBin) = JdbcDb.serialize(e._2)
        Some((e._1, format, entryBin, e._3))
    })
  }

  private[repos] class Ix3Table[Id: IdMapper : ClassTag, R: BaseColumnType](tag: Tag, ix3TableName: String) extends Table[(Id, R, Long)](tag, ix3TableName) {
    private val idIndexName: String = "ix3_id_" + DigestUtils.sha1Hex(ix3TableName)
    private val valueIndexName: String = "ix3_value_" + DigestUtils.sha1Hex(ix3TableName)
    private val parentPkIndexName: String = "ix3_ppk_" + DigestUtils.sha1Hex(ix3TableName)

    def id = column[Id]("id")(columnMapper[Id])

    def value = column[R]("value")

    // We build an index on the parentPk to make it efficient to find rows in the log table
    // which are not indexed (we look up this table for a lack of a record with a given pk)
    def parentPk = column[Long]("parent_pk")

    def idIndex = index(idIndexName, id)

    // Ensure all rows are unique
    def uniqueEntriesIdx = index(valueIndexName, (value, id, parentPk), unique = true)

    def parentPkIndex = index(parentPkIndexName, parentPk)

    def * = (id, value, parentPk)
  }

  private val repoMap: collection.concurrent.Map[String, RepoImpl[_, _]] = collection.concurrent.TrieMap.empty

  private[repos] def innerRepo[Id, M](repo: Repo[Id, M]): RepoImpl[Id, M] = {
    repoMap.getOrElse(repo.name, {
      val newRepo = new RepoImpl[Id, M](repo)(repo.idClass, repo.mClass, repo.dataMapper, repo.idMapper)
      repoMap += (repo.name -> newRepo)
      newRepo
    }).asInstanceOf[RepoImpl[Id, M]]
  }

  private[repos] def innerIndex[Id, M, R](index: SecondaryIndex[Id, M, _]) = {
    val repoImpl = innerRepo(index.repo)
    repoImpl.indexMap(index.name).asInstanceOf[repoImpl.IndexTableImpl[R]]
  }

  def runRepoAction[S <: repos.NoStream, Id, M, R](e: RepoAction[S, Id, M, R], ctx: Context)(implicit ec: ExecutionContext): Future[R] = e match {
    case CreateAction(repo) =>
      innerRepo(repo).create
    case InsertAction(repo, entries) =>
      innerRepo(repo).insert(entries).map(_ => ())
    case GetAction(repo, id) =>
      innerRepo(repo).get(id)
        .flatMap {
          case Some(o) => Future.successful(o.asInstanceOf[R])
          case None => Future.failed(new ElementNotFoundException(repo.idMapper.toUUID(id).toString))
        }
    case MultiGetAction(repo, ids) =>
      innerRepo(repo).multiGet(ids).asInstanceOf[Future[R]]
    case GetLastEntry(repo) =>
      innerRepo(repo).lastEntry()
    case DeleteAction(repo, ids) =>
      innerRepo(repo).delete(ids).asInstanceOf[Future[R]]
    case GetEntriesAction(repo, fromPk, idsConstraint, excludePks, afterTimeMsec, count) =>
      innerRepo(repo).getEntries(fromPk = fromPk, idsConstraint = idsConstraint,
        excludePks = excludePks, afterTimeMsec = afterTimeMsec, count = count, ctx = ctx)
    case GetAllLatestEntriesAction(repo) =>
      innerRepo(repo).allLatestEntries(ctx)
    case IndexGetAllAction(index, criteria, offset, count) =>
      innerIndex(index).all(criteria, offset, count, ctx)
    case IndexCountAction(index, criteria) =>
      innerIndex(index).count(criteria)
    case IndexAggegrationAction(index, agg) =>
      innerIndex(index).aggregate(agg)
  }

  private[repos] lazy val jc = new JanitorComponent(profile, db)

  /** Finds the last pk the scanner has checkpointed for (jobName, repo) */
  def scannerFindLastPk[Id, M](jobName: String, repo: Repo[Id, M]): Future[Option[Long]] =
    jc.scannerCheckpoints.findLastPk(jobName, repo)

  /** Update the last pk the scanner has reached. */
  def scannerUpdateCheckpoint[Id, M](jobName: String, repo: Repo[Id, M], lastPk: Long): Future[Unit] =
    jc.scannerCheckpoints.updateCheckpoint(jobName, repo, lastPk)
}

object JdbcDb {
  object EntryFormats {
    val BINARY = '1' // data serialized and encoded in binary form in entry_bin field.
    val SNAPPY = '2' // data serialized and compressed with Snappy in entry_bin field.
  }

  def parse[M](format: Char, data: Array[Byte])(implicit dataMapper: DataMapper[M]) = {
    val bytes = format match {
      case EntryFormats.BINARY => data
      case EntryFormats.SNAPPY => Snappy.uncompress(data)
      case _ => throw new RuntimeException("Unsupported entry format")
    }
    dataMapper.fromBytes(bytes)
  }

  def serialize[M](m: M)(implicit dataMapper: DataMapper[M]) = {
    val bytes = dataMapper.toBytes(m)
    if (bytes.length < 100) {
      (EntryFormats.BINARY, bytes)
    } else {
      (EntryFormats.SNAPPY, Snappy.compress(bytes))
    }
  }
}

