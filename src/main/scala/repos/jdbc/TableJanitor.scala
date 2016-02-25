package repos.jdbc

/** Table Janitor is a process that is responsible to ensure that all index tables
  * are being updated. It also populates new indexes.
  *
  * It is possible that one of the repo users is running an old binary that does not
  * write to some index on insert.  Table Janitor can ensure that everything that
  * is inserted gets eventually indexed.
  *
  * It is assumed that at any point in time there is at most one Table Janitor
  * running over a database.
  *
  * Periodically, the janitor scans for new entries in the main table and index
  * them if necessary.
  *
  * Sometimes, the PKs may not appear in consecutive order. For example a later transaction
  * completes before a transaction that starts earlier. Then, the Janitor will observe a temporary
  * gap in the pks. Since the Janitor wants to ensure that all entries are indexed, when gaps
  * in the PKs are detected, it will retry to fill them (for a configurable time period) until
  * they are fixed.
  *
  * The table janitor uses a JanitorIndexStatus table that maps index names to
  * the highest PK p on the main table where all pk <= p are known to be indexed (that is,
  * all pks up to the first unresolved gap)
  */

import akka.actor.{ActorRef, Actor, ActorLogging}
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import repos.{EntryTableRecord, Repo, SecondaryIndex}
import slick.lifted
import slick.lifted.BaseColumnExtensionMethods

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.language.existentials

class TableJanitor(readJdbcDb: JdbcDb, writeJdbcDb: JdbcDb, allRepos: Seq[Repo[_, _]],
                   materializer: akka.stream.Materializer, monitorActor: Option[ActorRef]) extends Actor with ActorLogging {

  import TableJanitor._
  import context.dispatcher

  // We care only about repos that have indexes.
  val repos = allRepos.filter(_.allIndexes.nonEmpty)

  var janitorStatus = TableJanitorStatus()

  def updateStatus(newJanitorStatus: TableJanitorStatus): Unit = {
    janitorStatus = newJanitorStatus
    monitorActor.foreach(_ ! janitorStatus)
  }

  /** Build indexes from the last checkpoint to the present.
    *
    * @return number of index records written.
    */
  def catchUp()(implicit ec: scala.concurrent.ExecutionContext): Long = {
    implicit def am: Materializer = materializer

    val currentTime = System.currentTimeMillis()

    val result = repos.map {
      repo =>
        updateStatus(janitorStatus.copy(current = s"Catching up on ${repo.name}"))
        val oldState = janitorStatus.repoState(repo.name)
        val newState = catchUpForRepo(
          readJdbcDb = readJdbcDb,
          writeJdbcDb = writeJdbcDb,
          currentTime = currentTime,
          statusTable = janitorStatus.statusTable,
          repo = repo,
          state = oldState,
          log = log.info)

        updateStatus(
          janitorStatus.copy(
            repoState = janitorStatus.repoState + (repo.name -> newState)))
        newState.indexedAllUpTo - oldState.indexedAllUpTo
    }.sum
    updateStatus(janitorStatus.copy(current = "Idle"))
    result
  }

  /** Creates Janitor's index table, missing repos and index tables if they do not exist. */
  def setupTables()(implicit ec: scala.concurrent.ExecutionContext) = {
    RepoManagement.createMissingRepos(writeJdbcDb, allRepos)

    // Ensure that all indexes not in the index status are set to zero.
    val currentStatus = loadJanitorIndexStatus(readJdbcDb = readJdbcDb)
    for {
      repo <- allRepos
      index <- repo.allIndexes if (!currentStatus.contains(readJdbcDb.innerIndex(index).ix3TableName))
    } {
      updateLastPkForTable(writeJdbcDb, index, 0)
    }
  }

  def initJanitor(): Unit = {
    log.info("Booting Table Janitor.")
    setupTables()
    val statusTable = loadJanitorIndexStatus(readJdbcDb = readJdbcDb)

    updateStatus(TableJanitorStatus(
      repoState = repos.map {
        repo =>
          val min = repo.allIndexes.map(lookupInStatusTable(readJdbcDb, statusTable, _)).min
          repo.name -> State(min, min, Vector.empty)
      }.toMap,
      statusTable = statusTable,
      current = "Initialized"))


    log.info("Starting to catch up.")
    val updatedRecords = catchUp()
    log.info(s"Done catching up. $updatedRecords records updated.")
  }

  // Boots the Janitor.
  override def preStart: Unit = {
    self ! Init
  }

  def receive = {
    case Init =>
      initJanitor()
    case Tick =>
      updateStatus(janitorStatus.copy(statusTable = loadJanitorIndexStatus(readJdbcDb = readJdbcDb)))
      catchUp()
  }

  override def postStop: Unit = {
    log.info("Stopping Table Janitor.")
  }
}

object TableJanitor {
  val JANITOR_INDEX_STATUS_TABLE = "janitor_index_status"

  case object Tick

  case object Init

  case class Gap(start: Long, end: Long, observed: Long) {
    override def toString: String = s"[$start, $end] at $observed"
  }

  /** State of a run
    *
    * @param indexedAllUpTo the maximal pk that we are confident we indexed it and everything before it.
    * @param maxSeen the last pk observed.
    * @param gaps Set of pk ranges between seenAllUp to maxSeen we have not seen.
    */
  case class State(indexedAllUpTo: Long, maxSeen: Long, gaps: Vector[Gap])

  case class TableJanitorStatus(
       repoState: Map[String, State] = Map.empty,
       statusTable: StatusTable = Map.empty,
       current: String = "Initializing")

  val FORGET_MISSING_AFTER_MS = 600 * 1000

  val GROUP_LIMIT = 5000

  def processGroup[Id, M](state: State, items: Seq[EntryTableRecord[Id, M]]): State = {
    require(state.maxSeen == state.indexedAllUpTo || state.gaps.nonEmpty)
    require(state.gaps.isEmpty || state.gaps.head.start == state.indexedAllUpTo + 1)
    require(state.gaps.isEmpty || state.gaps.last.end < state.maxSeen)
    val tmpState = items.foldLeft(state) {
      case (s, item) =>
        assert(item.pk > s.maxSeen)
        if (item.pk == s.maxSeen + 1)
          s.copy(maxSeen = item.pk)
        else
          s.copy(maxSeen = item.pk, gaps = s.gaps :+
            Gap(s.maxSeen + 1, item.pk - 1, item.timestamp))
    }
    if (tmpState.gaps.isEmpty) State(tmpState.maxSeen, tmpState.maxSeen, Vector.empty)
    else tmpState.copy(indexedAllUpTo = tmpState.gaps.head.start - 1)
  }

  private[repos] def nextEntries[Id, M, R](readJdbcDb: JdbcDb, writeJdbcDb: JdbcDb, repo: Repo[Id, M], initialState: State, indexItems: (State, Seq[EntryTableRecord[Id, M]]) => Unit)(implicit am: Materializer, ec: ExecutionContext): State = {
    Await.result(
      Source.fromPublisher(readJdbcDb.stream(repo.getEntries(initialState.maxSeen)))
        .grouped(GROUP_LIMIT).runFold(initialState) { (state, items) =>
        val newState = processGroup(state, items)
        indexItems(newState, items)
        newState
      }, Duration.Inf)
  }

  private[repos] def fillGaps[Id, M](gaps: Seq[Gap], items: Set[Long]): Seq[Gap] = {
    @tailrec
    def inner(original: Gap, current: Long, acc: List[Gap]): List[Gap] = {
      if (current > original.end) acc
      else if (!items.contains(current)) acc match {
        case (g@Gap(start, end, _)) :: gs if current == end + 1 =>
          // extending current gap
          inner(original, current + 1, g.copy(end = current) :: gs)
        case gs =>
          // new gap
          inner(original, current + 1, Gap(current, current, original.observed) :: gs)
      } else inner(original, current + 1, acc)
    }

    gaps.flatMap(g => inner(g, g.start, Nil).reverseIterator)
  }

  private[repos] def fetchGaps[Id, M](jdbcDb: JdbcDb, repo: Repo[Id, M], gaps: Seq[Gap],
                                      indexItems: Seq[EntryTableRecord[Id, M]] => Unit)(implicit am: Materializer): Seq[Gap] =
  if (gaps.isEmpty) gaps else {
    import jdbcDb.profile.api._
    def inBetween(g: Gap)(pk: BaseColumnExtensionMethods[Long]) = if (g.start == g.end)
      (pk === g.start) else pk.between(g.start, g.end)

    val allGaps = gaps.tail.foldLeft(inBetween(gaps.head) _) {
      case (filter, gap) =>
        pk: BaseColumnExtensionMethods[Long] => filter(pk) || inBetween(gap)(pk)
    }

    val q = jdbcDb.innerRepo(repo).entryTable.filter(e => allGaps(e.pk)).sortBy(_.pk).result
    Await.result(
      Source.fromPublisher(jdbcDb.db.stream(q)).grouped(5000).runFold(gaps) {
        (gaps, items) =>
          indexItems(items)
          fillGaps(gaps, items.map(_.pk).toSet)
      }, Duration.Inf)
  }

  type IndexableRecord[Id, M] = ((Id, M), Long)

  private[repos] def ensureIndexed[Id, M, R](jdbcDb: JdbcDb, indexTable: SecondaryIndex[Id, M, R],
                                             entries: Seq[IndexableRecord[Id, M]],
                                             log: String => Unit = _ => ()) = {
    import jdbcDb.profile.api._
    val inner = jdbcDb.innerIndex(indexTable)
    val alreadyIndexedPk: Set[Long] =
      jdbcDb.jc.blockingWrapper(
        inner.indexTable.asInstanceOf[lifted.TableQuery[JdbcDb#Ix3Table[Id, R]]]
          .filter(_.parentPk inSet (entries.map(_._2))).map(_.parentPk).result).toSet
    val unindexedEntries = entries.filterNot(e => alreadyIndexedPk.contains(e._2))
    if (unindexedEntries.nonEmpty) {
      jdbcDb.jc.blockingWrapper(inner.buildInsertAction(unindexedEntries))
      log(s"Repo ${indexTable.repo.name}: indexed ${unindexedEntries.size} entries into ${indexTable.name}")
    }
  }

  private type StatusTable = Map[String, Long]

  private def lookupInStatusTable(readJdbcDb: JdbcDb, s: StatusTable, index: SecondaryIndex[_, _, _]): Long = {
    s(readJdbcDb.innerIndex(index).ix3TableName)
  }

  private def discardExpiredGaps(repoName: String, gaps: Seq[Gap], currentTime: Long, logger: String => Unit): Seq[Gap] = {
    gaps.filterNot {
      g =>
        val shouldExpire = g.observed + FORGET_MISSING_AFTER_MS < currentTime
        if (shouldExpire) {
          logger(s"Repo ${repoName}: discarding expired gap $g")
        }
        shouldExpire
    }
  }

  // Loads the Janitor index status table to memory.
  private[repos] def loadJanitorIndexStatus(readJdbcDb: JdbcDb)(implicit ec: scala.concurrent.ExecutionContext): StatusTable = {
    import readJdbcDb.jc.profile.api._
    readJdbcDb.jc.blockingWrapper(
      readJdbcDb.jc.JanitorIndexStatus.map({
        r => r.indexTableName -> r.lastPk
      }).result).toMap
  }

  private def updateLastPkForTable(jdbcDb: JdbcDb, index: SecondaryIndex[_, _, _], lastPk: Long) = {
    jdbcDb.jc.JanitorIndexStatus.updateLastPkForIndex(jdbcDb.innerIndex(index).ix3TableName, lastPk)
  }

  private[repos] def catchUpForRepo[Id, M](readJdbcDb: JdbcDb,
                                           writeJdbcDb: JdbcDb,
                                           currentTime: Long,
                                           statusTable: StatusTable,
                                           repo: Repo[Id, M],
                                           state: State,
                                           log: String => Unit = println)(implicit materializer: Materializer, ec: ExecutionContext): State = {
    def indexItems(items: Seq[EntryTableRecord[Id, M]]): Unit = {
      val indexableItems: Seq[IndexableRecord[Id, M]] = items.map(i => ((i.id, i.entry), i.pk))

      repo.allIndexes.foreach {
        index =>
          val indexCompleteUpTo = lookupInStatusTable(readJdbcDb, statusTable, index)
          if (items.last.pk > indexCompleteUpTo) {
            TableJanitor.ensureIndexed(writeJdbcDb, index,
              indexableItems.filter(_._2 > indexCompleteUpTo), log)
          }
      }
    }

    val withoutDiscardedGaps: Seq[Gap] = discardExpiredGaps(repo.name, state.gaps, currentTime, log)
    val updatedGaps: Seq[Gap] = TableJanitor.fetchGaps(jdbcDb = readJdbcDb, repo, withoutDiscardedGaps,
      indexItems)(materializer)

    val stateWithUpdatedGaps = state.copy(
      indexedAllUpTo = if (updatedGaps.isEmpty) state.maxSeen else (updatedGaps.head.start - 1),
      gaps = updatedGaps.toVector)

    val newState = TableJanitor.nextEntries(readJdbcDb, writeJdbcDb, repo, stateWithUpdatedGaps,
      { (tmpState, items: Seq[EntryTableRecord[Id, M]]) =>
        indexItems(items)
        repo.allIndexes.foreach {
          index =>
            if (tmpState.indexedAllUpTo > lookupInStatusTable(readJdbcDb, statusTable, index)) {
              updateLastPkForTable(jdbcDb = writeJdbcDb, index, tmpState.indexedAllUpTo)
            }
        }
      })

    if (newState.indexedAllUpTo != state.indexedAllUpTo) {
      repo.allIndexes.foreach {
        index =>
          if (newState.indexedAllUpTo > lookupInStatusTable(readJdbcDb, statusTable, index)) {
            updateLastPkForTable(jdbcDb = writeJdbcDb, index, newState.indexedAllUpTo)
          }
      }
    }

    {
      val currentGapSet = newState.gaps.toSet
      val previousGapSet = withoutDiscardedGaps.toSet
      val deletedGaps = previousGapSet -- currentGapSet
      val newGaps = currentGapSet -- previousGapSet
      if (deletedGaps.nonEmpty) {
        log(s"Repo ${repo.name}: closed gaps ${deletedGaps.mkString(", ")}")
      }
      if (newGaps.nonEmpty) {
        log(s"Repo ${repo.name}: detected new gaps ${newGaps.mkString(", ")}")
      }
    }

    newState
  }

}
