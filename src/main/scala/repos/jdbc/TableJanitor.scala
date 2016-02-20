package repos.jdbc

/** Table Janitor is a process that is responsible to ensure that the index tables
  * are being synchronized with the main entry tables.
  *
  * It is assumed that at any point in time there is at most one Table Janitor
  * running over a database.
  *
  * The Table Janitor owns a database table where it periodically records its state
  * in case the system is restarted. That database table maps index tables to the
  * highest entry's pk that has been indexed in them.
  *
  * At this point, for simplicity sake, this is how the table janitor operates:
  *
  * At start up time and on each Tick message:
  * 1. loads the state table.
  * 2. for each index table, scan through the log table starting at the latest pk in
  * the status table (or from the beginning if it is not there). This allows adding new indexes and have them
  * built automatically.
  *
  * TODO(nadavsr):
  * MISSING FUNCTIONALITY:
  * - monitor / recover crashes of the janitor.
  * - make a full index rebuild occasionally (once a day?)
  * - clean up index tables. There may be duplicate records.
  * - clean up entry tables (for instance, limit revisions for the same id)
  */

import akka.actor.Actor
import akka.event.Logging
import akka.stream.Materializer
import repos.{SecondaryIndex, Repo, Database}
import slick.driver.JdbcDriver
import slick.lifted

import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, Future }
import scala.language.existentials

class TableJanitor(jdbcDb: JdbcDb, repos: Seq[Repo[_, _]], materializer: akka.stream.Materializer) extends Actor {
  val log = Logging(context.system, this)

  val jc = jdbcDb.jc
  import jc.profile.api._
  import TableJanitor._
  import context.dispatcher

  type StatusTable = Map[String, Long]


  // Loads the Janitor index status table to memory. Inserts to the db table new records for
  // indexes we are building for the first time.
  def loadJanitorIndexStatus(implicit ec: scala.concurrent.ExecutionContext): StatusTable = {
    jc.blockingWrapper(
      jc.JanitorIndexStatus.map({
        r => r.indexTableName -> r.lastPk
      }).result).toMap
  }

  /** Build indexes from the last checkpoint to the present.
    *
    * @return number of index records written.
    */

  def catchUp(indexStatus: StatusTable)(implicit ec: scala.concurrent.ExecutionContext): Long = {
    implicit def am: Materializer = materializer

    def catchUpForRepo[TypeId, T](repo: Repo[TypeId, T]): Long = {
      val indexTables = repo.allIndexes
      indexTables.map {
        indexTable =>
          val startPk = indexStatus.getOrElse(jdbcDb.innerIndex(indexTable).ix3TableName, -1L)
          indexEntries(jdbcDb)(repo, startPk)(indexTable)
      }.sum
    }

    repos.map(catchUpForRepo(_)).sum
  }

  /** Creates Janitor's index table, missing repos and index tables if they do not exist. */
  def setupTables(appDb: JanitorComponent, repos: Seq[Repo[_, _]])(
    implicit ec: scala.concurrent.ExecutionContext) = {
    val newIndexes = RepoManagement.createMissingRepos(jdbcDb, repos)
    // If an index table is new we force re-indexing by setting highest pk to be -1.
    // This ensures that if we drop an index table it will get rebuilt
    // from scratch the next time we run, regardless of what was the original value
    // in the index status table.
    newIndexes.foreach {
      tableName =>
        jc.JanitorIndexStatus.updateLastPkForIndex(jdbcDb.db, tableName, -1)
    }
  }

  // Boots the Janitor.
  override def preStart: Unit = {
    log.info("Booting Table Janitor.")
    setupTables(jc, repos)
    val status: StatusTable = loadJanitorIndexStatus
    val updatedRecords = catchUp(status)
    log.info(s"Done catching up. $updatedRecords records updated.")
  }

  def receive = {
    case Tick =>
      catchUp(loadJanitorIndexStatus)
      sender ! Ok
  }

  override def postStop: Unit = {
    log.info("Stopping Table Janitor.")
  }
}

object TableJanitor {
  val JANITOR_INDEX_STATUS_TABLE = "janitor_index_status"

  case object Tick

  case object Ok

  private[repos] def unindexedEntries[Id, M, R](jdbcDb: JdbcDb)(
    repo: Repo[Id, M], startPk: Long)(indexTable: SecondaryIndex[Id, M, R]) = {
    import jdbcDb.profile.api._
    val it: lifted.TableQuery[JdbcDb#Ix3Table[Id, R]] = jdbcDb.innerIndex(indexTable).indexTable.asInstanceOf[lifted.TableQuery[JdbcDb#Ix3Table[Id, R]]]
    jdbcDb.innerRepo(repo).entryTable
      .filter(_.pk > startPk)
      .filterNot(_.pk in it.map(_.parentPk))
      .result
  }

  case class IndexResult(count: Long, maxPkSeen: Long)

  def indexEntries[Id, M, R](jdbcDb: JdbcDb)(
    repo: Repo[Id, M], startPk: Long)(indexTable: SecondaryIndex[Id, M, R])(implicit am: akka.stream.Materializer): Long = {
    val innerIndex: JdbcDb#RepoImpl[Id, M]#IndexTableImpl[R] = jdbcDb.innerIndex(indexTable).asInstanceOf[JdbcDb#RepoImpl[Id, M]#IndexTableImpl[R]]

    val f: Future[IndexResult] = akka.stream.scaladsl.Source.fromPublisher(
      jdbcDb.db.stream(unindexedEntries(jdbcDb)(repo, startPk)(indexTable))
    ).grouped(5000).runFold(IndexResult(0, startPk)) {
      (indexResult, group) =>
        val items = group.map(p => ((p.id, p.entry), p.pk))
        jdbcDb.jc.blockingWrapper(innerIndex.buildInsertAction(items))
        println(s"Indexed ${group.size} entries into ${innerIndex.ix3TableName}")
        val maxPkSeen = indexResult.maxPkSeen max group.map(_.pk).max
        if (maxPkSeen != indexResult.maxPkSeen) {
          jdbcDb.jc.JanitorIndexStatus.updateLastPkForIndex(
            jdbcDb.db, innerIndex.ix3TableName, maxPkSeen)
        }
        indexResult.copy(
          count = indexResult.count + group.size,
          maxPkSeen = maxPkSeen)
    }
    Await.result(f, Duration.Inf).count
  }

}
