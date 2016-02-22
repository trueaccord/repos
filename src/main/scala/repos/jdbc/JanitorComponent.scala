package repos.jdbc

import repos.Repo

import scala.async.Async.{ async, await }
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import slick.driver.{JdbcDriver, JdbcProfile}

case class IndexStatusRecord(pk: Option[Long] = None, indexTableName: String, lastPk: Long)

case class ScannerCheckpoint(jobName: String, repoName: String, lastPk: Long)

class JanitorComponent(val profile: JdbcProfile, db: JdbcProfile#Backend#Database) {
  import profile.api._

  private[repos] def blockingWrapper[A](f: DBIOAction[A, slick.dbio.NoStream, Nothing]) = Await.result(db.run(f), Duration.Inf)

  // Maps an index table name to the highest pk (of the entries table) that was indexed in it.
  class JanitorIndexStatusTable(tag: Tag) extends Table[IndexStatusRecord](tag, TableJanitor.JANITOR_INDEX_STATUS_TABLE) {
    def pk = column[Long]("pk", O.AutoInc, O.PrimaryKey)

    def indexTableName = column[String]("index_table")

    def lastPk = column[Long]("last_pk")

    // At most one record for (entry_table, index_table) pair.
    def idx = index("idx_entry_table", indexTableName, unique = true)

    def * = (pk.?, indexTableName, lastPk) <> (IndexStatusRecord.tupled, IndexStatusRecord.unapply)
  }

  object JanitorIndexStatus extends TableQuery[JanitorIndexStatusTable](new JanitorIndexStatusTable(_)) {

    def updateLastPkForIndex(indexTableName: String, lastPk: Long): Unit = {

      // Update existing row if exists
      val updateCount = blockingWrapper(JanitorIndexStatus.filter(
        r => r.indexTableName === indexTableName).map(_.lastPk).update(lastPk))
      // or insert new one if it doesn't.
      if (updateCount == 0) {
        blockingWrapper(JanitorIndexStatus += (IndexStatusRecord(None, indexTableName, lastPk)))
      }
    }
  }

  val ScannerCheckpointsTableName = "scanner_checkpoints"

  /** Table that holds the last pk a scanner has reached in repo */
  class ScannerCheckpointsTable(tag: Tag) extends Table[ScannerCheckpoint](tag, ScannerCheckpointsTableName) {
    import scala.async.Async.{ async, await }
    /** Identifies the scanning process (there may be more than one process scanning the same repo. */
    def jobId = column[String]("job_id")

    def repoName = column[String]("repo_name")

    def lastPk = column[Long]("last_pk")

    // At most one record for (repoName, jobId) pair.
    def idx = index("idx_job_repo", (jobId, repoName), unique = true)

    def * = (jobId, repoName, lastPk) <> (ScannerCheckpoint.tupled, ScannerCheckpoint.unapply)
  }

  object scannerCheckpoints extends TableQuery(new ScannerCheckpointsTable(_)) {
    import scala.concurrent.ExecutionContext.Implicits.global

    private def findCheckpoint(jobName: String, repo: Repo[_, _]) = filter {
      r =>
        r.jobId === jobName && r.repoName === repo.name
    }

    /** Finds the last pk the scanner has checkpointed for (jobName, repo) */
    def findLastPk(jobName: String, repo: Repo[_, _]): Future[Option[Long]] = {
      val query = findCheckpoint(jobName, repo)
        .map(_.lastPk)
        .result.headOption
      db.run(query)
    }

    /** Update the last pk the scanner has reached. */
    def updateCheckpoint(jobName: String, repo: Repo[_, _], lastPk: Long): Future[Unit] = async {
      val updateCount: Int = await(
        db.run(findCheckpoint(jobName, repo)
          .filter(_.lastPk < lastPk) // to ensure we only update if necessary
          .map(_.lastPk).update(lastPk)))

      if (updateCount == 0) {
        // insert new record
        await(db.run(this += ScannerCheckpoint(jobName, repo.name, lastPk)))
      } else {
        ()
      }
    }
  }
}
