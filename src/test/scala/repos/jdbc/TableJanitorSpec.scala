package repos.jdbc

import java.util.UUID

import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, MustMatchers}
import repos.testutils.FooId
import repos.EntryTableRecord
import repos.jdbc.TableJanitor.{State, Gap}

class TableJanitorSpec extends FlatSpec with MustMatchers with PropertyChecks {
  def entry(pk: Long, ts: Long): EntryTableRecord[FooId, String] =
    EntryTableRecord(pk, FooId(UUID.randomUUID()), ts, "")

  "processGroup" must "observe gaps" in {
    val s = State(30, 30, Vector.empty)
    val newState = TableJanitor.processGroup(s, Seq(entry(31, 100), entry(32, 100), entry(35,100), entry(37, 200)))
    newState must be(State(32, 37, Vector(Gap(33, 34, 100), Gap(36, 36, 200))))
  }

  "fillGaps" must "fill gaps" in {
    val gaps = Seq(Gap(17, 35, 0), Gap(39, 42, 0), Gap(43, 47, 0))
    TableJanitor.fillGaps(gaps, (1L to 50L).toSet) must be(Seq.empty)
    TableJanitor.fillGaps(gaps, (1L to 45L).toSet) must be(
      Seq(Gap(46, 47, 0)))
    TableJanitor.fillGaps(gaps, (41L to 45L).toSet) must be(
      Seq(Gap(17, 35, 0), Gap(39, 40, 0), Gap(46, 47, 0)))
    TableJanitor.fillGaps(gaps, (41L to 47L).toSet) must be(
      Seq(Gap(17, 35, 0), Gap(39, 40, 0)))
    TableJanitor.fillGaps(gaps, Set(18L, 40L, 41L, 45L, 47L)) must be(
      Seq(Gap(17, 17, 0), Gap(19, 35, 0), Gap(39, 39, 0), Gap(42, 42, 0), Gap(43, 44, 0), Gap(46, 46, 0)))
  }

  def offsetGap(gap: Gap, o: Long) = gap.copy(start = gap.start + o, end = gap.end + o)

  def genGap: Gen[Gap] = for {
    s <- Gen.chooseNum(0L, 30L)
    e <- Gen.chooseNum(0L, 30L)
    ts <- Gen.posNum[Long]
  } yield Gap(s, s + e, ts)

  def genGaps: Gen[Vector[Gap]] = for {
    gaps <- Gen.listOf(genGap)
  } yield {
    val offsets = gaps.scanLeft(0L)((a, t) => a + t.end)
    (gaps zip offsets).map {
      case (g, o) => offsetGap(g, o)
    }.toVector
  }

  def genStateNoGaps: Gen[State] = for {
    indexedAllUpTo <- Gen.choose(0L, 1000000L)
  } yield State(indexedAllUpTo, indexedAllUpTo, Vector.empty)

  def genState: Gen[State] = for {
    gaps <- genGaps
    indexedAllUpTo <- gaps.headOption.map(g => Gen.const(g.start - 1L)).getOrElse(Gen.posNum[Long])
    minForMaxSeen = gaps.lastOption.map(_.end + 1).getOrElse(indexedAllUpTo)
    maxForMaxSeen = if (gaps.isEmpty) gaps.lastOption.map(_.end + 1).getOrElse(indexedAllUpTo)
    maxSeen <- if (gaps.isEmpty) Gen.const(indexedAllUpTo)
      else Gen.choose(gaps.last.end + 1, gaps.last.end + 50)
  } yield State(indexedAllUpTo = indexedAllUpTo, maxSeen = maxSeen, gaps = gaps)

  def enumerateGaps(gaps: Seq[Gap]): Seq[Long] = gaps.flatMap(g => (g.start to g.end))

  def verifyIncreasingGaps(gaps: Seq[Gap]): Unit = {
    if (gaps.size > 1) {
      gaps.sliding(2).foreach {
        case Vector(g1, g2) => (g2.start) must be >= (g1.end)
      }
    }
  }

  def genScenario = for {
    state <- genState
    items <- Gen.someOf((state.maxSeen + 1L) to (state.maxSeen + 1000L)).map(_.map(entry(_, 101)))
  } yield (state, items)

  def genScenarioNoGaps = for {
    state <- genStateNoGaps
    items <- Gen.someOf((state.maxSeen + 1L) to (state.maxSeen + 1000L)).map(_.map(entry(_, 101)))
  } yield (state, items)

  "processGroup" must "return correct state" in {
    forAll(genScenario) {
      case (state, items) =>
        whenever(items.size >0) {
          val newState = TableJanitor.processGroup(state, items)
          state.gaps.toSet.subsetOf(newState.gaps.toSet) must be(true)
          newState.maxSeen must be(items.map(_.pk).max)

          val modelA = items.map(_.pk).toSet
          val modelB = ((state.maxSeen + 1) to newState.maxSeen).toSet -- enumerateGaps(newState.gaps)
          modelA must be(modelB)
          val newIndexedAllUpTo: Long = Stream.from(state.indexedAllUpTo.toInt + 1).map(_.toLong).takeWhile(modelA.contains)
            .lastOption.getOrElse(state.indexedAllUpTo)
          newState.indexedAllUpTo must be(newIndexedAllUpTo)

          if (newState.gaps.isEmpty) {
            newState.indexedAllUpTo must be(newState.maxSeen + 1)
          }
        }
    }
  }

  "processGroup" must "be invariant for processing items in groups" in {
    forAll(genScenario) {
      case (state, items) =>
        TableJanitor.processGroup(state, items) must be
          items.grouped(100).foldLeft(state)(TableJanitor.processGroup)
    }
  }

  "genGaps" must "generate non-intersecting and increasing gaps" in {
    forAll(genGaps) {
      gaps =>
        verifyIncreasingGaps(gaps)
    }
  }

  "fillGaps" must "work on arbitrary sets" in {
    forAll(genGaps) {
      gaps =>
        forAll(Gen.listOf(Gen.choose(0L, gaps.lastOption.map(_.end).getOrElse(0L) + 100L)).map(_.toSet)) {
          items =>
            val newGaps = TableJanitor.fillGaps(gaps, items)
            verifyIncreasingGaps(newGaps)
            val oldEnum = enumerateGaps(gaps).toSet
            val newEnum = enumerateGaps(newGaps).toSet

            newEnum.subsetOf(oldEnum) must be(true)
            (oldEnum -- newEnum).subsetOf(items) must be(true)
        }
    }
  }
}
