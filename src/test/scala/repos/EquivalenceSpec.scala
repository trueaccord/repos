package repos

import java.util.UUID

import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import repos.Action._
import repos.SecondaryIndexQueries._
import repos.inmem.InMemDb
import repos.testutils.TestUtils.await
import repos.testutils.{FooId, FooRepo, TestUtils}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}


object GenLookups {
  val oneCharString = Gen.alphaChar.map(_.toString)
  val twoCharString = Gen.listOfN(2, Gen.alphaChar).map(_.mkString)

  def genRInRange[Id, M, R: Ordering](index: SecondaryIndex[Id, M, R])(rGen: Gen[R]): Gen[InRange[R]] = for {
    start <- rGen
    end <- rGen
  } yield ExpectLookupCriteria[R](index).inRange(start, end)

  def genRInSet[Id, M, R: Ordering](index: SecondaryIndex[Id, M, R])(rGen: Gen[R]): Gen[InSet[R]] = {
    for {
      rs <- Gen.nonEmptyListOf(rGen)
    } yield ExpectLookupCriteria[R](index).inSet(rs.toSet)
  }

  def genRLargerThan[Id, M, R: Ordering](index: SecondaryIndex[Id, M, R])(rGen: Gen[R]): Gen[LargerThan[R]] = for {
    start <- rGen
  } yield ExpectLookupCriteria[R](index).largerThan(start)

  def genRLargerThanOrEqual[Id, M, R: Ordering](index: SecondaryIndex[Id, M, R])(rGen: Gen[R]): Gen[LargerThanOrEqual[R]] = for {
    start <- rGen
  } yield ExpectLookupCriteria[R](index).largerThanOrEqual(start)

  def genRSmallerThan[Id, M, R: Ordering](index: SecondaryIndex[Id, M, R])(rGen: Gen[R]): Gen[SmallerThan[R]] = for {
    start <- rGen
  } yield ExpectLookupCriteria[R](index).smallerThan(start)

  def genRSmallerThanOrEqual[Id, M, R: Ordering](index: SecondaryIndex[Id, M, R])(rGen: Gen[R]): Gen[SmallerThanOrEqual[R]] = for {
    start <- rGen
  } yield ExpectLookupCriteria[R](index).smallerThanOrEqual(start)

  def genRMatching[Id, M, R: Ordering](index: SecondaryIndex[Id, M, R])(rGen: Gen[R]): Gen[Equals[R]] = for {
    s <- rGen
  } yield ExpectLookupCriteria[R](index).matching(s)

  def genStringStartsWith[Id, M](index: SecondaryIndex[Id, M, String])(rGen: Gen[String]): Gen[StartsWith[String]] = for {
    s <- rGen
  } yield ExpectLookupCriteria[String](index).startsWith(s)

  def genStringAnyLookup[Id, M](index: SecondaryIndex[Id, M, String]): Gen[LookupCriteria[String]] = Gen.oneOf(
    genRInRange(index)(Gen.alphaStr),
    genRInSet(index)(twoCharString),
    genRLargerThan(index)(Gen.alphaStr),
    genRLargerThanOrEqual(index)(Gen.alphaStr),
    genRSmallerThan(index)(Gen.alphaStr),
    genRSmallerThanOrEqual(index)(Gen.alphaStr),
    genRMatching(index)(Gen.oneOf(twoCharString, Gen.alphaStr)),
    genStringStartsWith(index)(Gen.oneOf(oneCharString, twoCharString, Gen.alphaStr)))

  def genLongAnyLookup[Id, M](index: SecondaryIndex[Id, M, Long]): Gen[LookupCriteria[Long]] = {
    val longGen = Gen.choose(-10L, 100L)
    Gen.oneOf(
      genRInRange(index)(longGen),
      genRInSet(index)(longGen),
      genRLargerThan(index)(longGen),
      genRLargerThanOrEqual(index)(longGen),
      genRSmallerThan(index)(longGen),
      genRSmallerThanOrEqual(index)(longGen),
      genRMatching(index)(longGen))
  }
}

object GenActions {
  def genInsertAction[Id, M](repo: Repo[Id, M], idSet: Seq[Id])(implicit genM: Gen[M]): Gen[InsertAction[Id, M]] = for {
    ids <- Gen.nonEmptyListOf(Gen.oneOf(idSet))
    values <- Gen.listOfN(ids.size, genM)
  } yield repo.insert((ids zip values): _*)

  def genGet[Id, M](repo: Repo[Id, M], idSet: Seq[Id]): Gen[GetAction[Id, M]] = for {
    value <- Gen.oneOf(idSet)
  } yield repo(value)

  def genMultiGet[Id, M](repo: Repo[Id, M], idSet: Seq[Id]): Gen[MultiGetAction[Id, M]] = for {
    ids <- Gen.nonEmptyListOf(Gen.oneOf(idSet))
  } yield repo.multiGet(ids)

  def genDelete[Id, M](repo: Repo[Id, M], idSet: Seq[Id]): Gen[DeleteAction[Id, M]] = for {
    ids <- Gen.nonEmptyListOf(Gen.oneOf(idSet))
  } yield repo.delete(ids.toSet)

  def genEntries[Id, M](repo: Repo[Id, M], idSet: Seq[Id]) = for {
    minPk <- Gen.choose(-1, 200)
    idsConstaint <- Gen.option(Gen.listOf(Gen.oneOf(idSet)))
    excludedPks <- Gen.listOf(Gen.choose(-20L, 50L)).map(_.toSet)
    count <- Gen.option(Gen.choose(0, 5))
  } yield repo.getEntries(minPk, idsConstaint, excludedPks, count = count).map(_.map(_.copy(timestamp = 0)))

  def genLastEntry[Id, M](repo: Repo[Id, M]): Gen[Action[NoStream, Option[EntryTableRecord[Id, M]]]] =
    Gen.const(repo.lastEntry().map(_.map(_.copy(timestamp = 0))))

  def genAllLatestEntries[Id, M](repo: Repo[Id, M]): Gen[GetAllLatestEntriesAction[Id, M]] =
    Gen.const(repo.allLatestEntries())

  def genIndexFind[Id, M, R](index: SecondaryIndex[Id, M, R])(
    genLookup: SecondaryIndex[Id, M, R] => Gen[LookupCriteria[R]]) = for {
    lookup <- genLookup(index)
    offset <- Gen.oneOf[Option[Int]](None, Gen.choose(0, 100).map(Some(_)))
    count <- Gen.oneOf[Option[Int]](None, Gen.choose(0, 100).map(Some(_)))
  } yield index.find(_ => lookup, offset, count)

  def genIndexCount[Id, M, R](index: SecondaryIndex[Id, M, R])(
    genLookup: SecondaryIndex[Id, M, R] => Gen[LookupCriteria[R]]) = for {
    lookup <- genLookup(index)
  } yield index.count(_ => lookup)

  def genIndexFirst[Id, M, R](index: SecondaryIndex[Id, M, R])(
    genLookup: SecondaryIndex[Id, M, R] => Gen[LookupCriteria[R]]) = for {
    lookup <- genLookup(index)
  } yield index.first(_ => lookup)

  def genAnyAction[Id, M](repo: Repo[Id, M], idSet: Seq[Id])(implicit genM: Gen[M]): Gen[Action[NoStream, Any]] = Gen.oneOf(
    genInsertAction(repo, idSet),
    genGet(repo, idSet),
    genMultiGet(repo, idSet),
    genEntries(repo, idSet),
    genDelete(repo, idSet),
    genLastEntry(repo),
    genAllLatestEntries(repo)
  )

  def genFooAction(idSet: Seq[FooId])(implicit genM: Gen[String]): Gen[Action[NoStream, Any]] = Gen.oneOf(
    genAnyAction(FooRepo, idSet),
    genIndexFind(FooRepo.firstTwoIndex)(GenLookups.genStringAnyLookup),
    genIndexCount(FooRepo.firstTwoIndex)(GenLookups.genStringAnyLookup),
    genIndexFirst(FooRepo.firstTwoIndex)(GenLookups.genStringAnyLookup),
    genIndexFind(FooRepo.lengthIndex)(GenLookups.genLongAnyLookup),
    genIndexCount(FooRepo.lengthIndex)(GenLookups.genLongAnyLookup),
    genIndexFirst(FooRepo.lengthIndex)(GenLookups.genLongAnyLookup)
  )
}


class EquivalenceSpec extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks {
  val ids: Vector[FooId] = Vector.fill(30)(FooId(UUID.randomUUID()))

  def waitAndCompare[T](f1: Future[T], f2: Future[T]) = {
    val t1 = Try(await(f1))
    val t2 = Try(await(f2))
    (t1, t2) match {
      case (Success(t1), Success(t2)) => t1 must be(t2)
      case (Failure(e1), Failure(e2)) => e1.getMessage must be(e2.getMessage)
      case (t1, t2) => t1 must be(t2)
    }
  }

  class Fixture {
    val jdb = TestUtils.makeH2DB()
    val jdbcDb = TestUtils.makeH2JdbcDb(jdb)

    val inMemDb = new InMemDb

    await(jdbcDb.run(FooRepo.create()))
    await(inMemDb.run(FooRepo.create()))
  }

  "Both databases" should "return the same results" in {
    forAll(Gen.nonEmptyListOf(GenActions.genFooAction(ids)(Gen.alphaStr))) {
      actions =>
        val fixture = new Fixture()
        import fixture._

        try {
          waitAndCompare(
            jdbcDb.run(Action.seq(actions)),
            inMemDb.run(Action.seq(actions)))
        } finally {
          jdb.shutdown
        }
    }
  }

}
