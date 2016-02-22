package repos.jdbc

import repos.{SecondaryIndex, Repo}
import slick.jdbc.meta.MTable

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.ExecutionContext.Implicits.global

object RepoManagement {
  def existingTables(jdbcDb: JdbcDb)(implicit ec: ExecutionContext): Set[String] = {
    jdbcDb.jc.blockingWrapper(MTable.getTables.map(_.map(_.name.name))).toSet
  }

  // Create all missing repos and index tables.
  def createMissingRepos(jdbcDb: JdbcDb,
                         repos: Seq[Repo[_, _]], log: String => Unit = println): Seq[SecondaryIndex[_, _, _]] = {
    val jc = jdbcDb.jc
    import jc.profile.api._

    import scala.concurrent.ExecutionContext.Implicits.global

    {
      val _tables = existingTables(jdbcDb)
      if (!_tables.contains(TableJanitor.JANITOR_INDEX_STATUS_TABLE)) {
        jc.blockingWrapper(jc.JanitorIndexStatus.schema.create)
      }
      if (!_tables.contains(jc.ScannerCheckpointsTableName)) {
        jc.blockingWrapper(jc.scannerCheckpoints.schema.create)
      }
    }

    val originalTables = existingTables(jdbcDb)

    def upgradeRepo(repo: Repo[_, _]): List[SecondaryIndex[_, _, _]] = {
      if (!originalTables.contains(repo.name)) {
        log(s"Creating ${repo.name}.")
        Await.result(jdbcDb.run(repo.create()), Duration.Inf)
      }
      val currentExistingTables = existingTables(jdbcDb)
      repo.allIndexes.collect {
        case indexTable if !currentExistingTables.contains(jdbcDb.innerIndex(indexTable).ix3TableName) =>
          log(s"Creating missing index table ${indexTable.name}.")
          jc.blockingWrapper(jdbcDb.innerIndex(indexTable).indexTable.schema.create)
          indexTable
      }
    }

    val parRepos = repos.par
    parRepos.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(4))
    parRepos.flatMap(upgradeRepo).seq
  }

  def dropAll(jdbcDb: JdbcDb, repos: Seq[Repo[_, _]]): Unit = {
    import jdbcDb.profile.api._
    val existingTablesInternal: Set[String] = existingTables(jdbcDb)
    if (existingTablesInternal.contains(jdbcDb.jc.ScannerCheckpointsTableName)) {
      jdbcDb.jc.blockingWrapper(
        jdbcDb.jc.scannerCheckpoints.schema.drop)
    }
    if (existingTablesInternal.contains(TableJanitor.JANITOR_INDEX_STATUS_TABLE)) {
      jdbcDb.jc.blockingWrapper(
        jdbcDb.jc.JanitorIndexStatus.schema.drop)
    }
    repos.par.foreach {
      repo =>
        val inner = jdbcDb.innerRepo(repo)
        if (existingTablesInternal.contains(inner.latestTableName)) {
          jdbcDb.jc.blockingWrapper(inner.latestEntryTable.schema.drop)
        }
        if (existingTablesInternal.contains(repo.name)) {
          jdbcDb.jc.blockingWrapper(inner.entryTable.schema.drop)
        }
        repo.allIndexes.collect {
          case indexTable if existingTablesInternal.contains(jdbcDb.innerIndex(indexTable).ix3TableName) =>
            jdbcDb.jc.blockingWrapper(jdbcDb.innerIndex(indexTable).indexTable.schema.drop)
        }
    }
  }

  // added by fabian@trueaccord.com 2014-09-05 as this is a heavy
  // query that we only need for maintainance i implemented it in
  // sql straight away
  // more or less adapted from here:
  // http://explainextended.com/2009/04/26/keeping-latest-rows-for-a-group/
  //
  // deleting from innodb tables is slow. space is actually only recovered after
  // OPTIMIZE TABLE, which essentially rebuilds the table from scratch.
  def cleanupOldVersionsSql[Id, M](jdbcDb: JdbcDb, repo: Repo[Id, M], numVersionsToKeep: Int) = {
    import jdbcDb.profile.api._
    val numVersionsToKeep0 = numVersionsToKeep - 1 //LIMIT starts counting at 0
    val tableName = repo.name
    val queryString: DBIO[Int] =
      sqlu"""
        DELETE QUICK l.*
        FROM #$tableName l
        JOIN (
             SELECT uuid,
                    COALESCE(
                      (
                      SELECT time_msec
                      FROM   #$tableName li
                      WHERE  li.uuid = dlo.uuid
                      ORDER BY
                        li.uuid DESC, li.time_msec DESC
                      LIMIT $numVersionsToKeep0, 1
                      ), CAST('0001-01-01' AS DATETIME)
                    ) AS mts,
                    COALESCE(
                      (
                      SELECT pk
                      FROM   #$tableName li
                      WHERE  li.uuid = dlo.uuid
                      ORDER BY
                        li.uuid DESC, li.time_msec DESC, li.pk DESC
                      LIMIT $numVersionsToKeep0, 1
                    ), -1) AS mpk
             FROM (
                  SELECT DISTINCT uuid
                  FROM #$tableName dl
                  ) dlo
             ) lo
        ON l.uuid = lo.uuid
        AND (l.time_msec, l.pk) < (mts, mpk)
        """
    val count = jdbcDb.jc.blockingWrapper(queryString)
    val optimizeQuery = sql"OPTIMIZE TABLE #$tableName".as[String]
    jdbcDb.jc.blockingWrapper(optimizeQuery)
    count
  }
}
