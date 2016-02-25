package repos

import java.util.UUID

import org.scalatest.{MustMatchers, OptionValues}
import repos.inmem.InMemDb
import repos.testutils.{FooRepo, FooId, TestUtils}
import scala.concurrent.ExecutionContext.Implicits.global

import TestUtils.await
import TestUtils.awaitStream


class RepoSpec extends org.scalatest.fixture.FlatSpec with MustMatchers with OptionValues {
  type FixtureParam = Database

  // Runs each tests against JDbcDb and InMemDb
  def withFixture(test: OneArgTest) = {
    val jdb = TestUtils.makeH2DB()
    val JdbcDb = TestUtils.makeH2JdbcDb(jdb)

    object InMemDb extends InMemDb

    await(JdbcDb.run(FooRepo.create()))
    await(InMemDb.run(FooRepo.create()))
    try {
      val o = test(JdbcDb)
      if (!o.isSucceeded) o
      else test(InMemDb)
    } finally {
      jdb.shutdown
    }
  }

  trait TestCase {
  }

  "test" should "work" in {
    db =>
      val id1 = FooId(UUID.randomUUID())
      val id2 = FooId(UUID.randomUUID())
      val z = FooRepo.insert(id1, "17")
      await(db.run(z))
      await(db.run(FooRepo(id1))) must be("17")
      intercept[ElementNotFoundException](await(db.run(FooRepo(id2))))
      await(db.run(FooRepo.insert(id2, "19")))
      await(db.run(FooRepo(id2))) must be("19")

      await(db.run(FooRepo.multiGet(Seq(id1, id2)))) must be(
        Map(id1 -> "17", id2 -> "19"))
  }

  "composite action" should "work" in {
    db =>
      val id1 = FooId(UUID.randomUUID())
      val id2 = FooId(UUID.randomUUID())
      val id3 = FooId(UUID.randomUUID())
      val a: Action[NoStream, Seq[Unit]] = Action.seq(Seq(
        FooRepo.insert(id1, "4"),
        FooRepo.insert(id2, "12")))
      await(db.run(a))
      await(db.run(FooRepo.multiGet(Seq(id1, id2, id3)))) must be(
        Map(id1 -> "4", id2 -> "12"))
  }

  "andThen" should "work" in {
    db =>
      val id1 = FooId(UUID.randomUUID())
      val id2 = FooId(UUID.randomUUID())
      await(db.run(FooRepo.insert(id1, "8") zip FooRepo.insert(id2, "17")))
      await(
        db.run(FooRepo(id1) zip FooRepo(id2))) must be(
        ("8", "17")
      )
  }

  "andThen" should "abort after first failure" in {
    db =>
      val id1 = FooId(UUID.randomUUID())
      val id2 = FooId(UUID.randomUUID())
      val c = FooRepo(id1) andThen FooRepo.insert(id2, "17")
      intercept[ElementNotFoundException](await(db.run(c)))
      intercept[ElementNotFoundException](await(db.run(FooRepo(id2))))
  }

  "flatMap" should "work" in {
    db =>
      val id1 = FooId(UUID.randomUUID())
      await(db.run(FooRepo.insert(id1, "8")))

      def doubleString(id: FooId) = FooRepo(id).flatMap {
        s: String =>
          FooRepo.insert(id, s + s)
      } andThen {
        FooRepo(id)
      }

      await(db.run(doubleString(id1))) must be("88")
  }

  "indexes" should "work" in {
    db =>
      val id1 = FooId(UUID.randomUUID())
      val id2 = FooId(UUID.randomUUID())
      val id3 = FooId(UUID.randomUUID())
      val id4 = FooId(UUID.randomUUID())
      val id5 = FooId(UUID.randomUUID())
      await(db.run(FooRepo.insert(id1, "8")))
      await(db.run(FooRepo.insert(id2, "14")))
      await(db.run(FooRepo.insert(id3, "275")))
      await(db.run(FooRepo.insert(id4, "35")))
      await(db.run(FooRepo.insert(id5, "ababa")))
      await(db.run(FooRepo.lengthIndex.allMatching(0))) must be(Seq())
      await(db.run(FooRepo.lengthIndex.allMatching(1))) must be(Seq(id1 -> "8"))
      await(db.run(FooRepo.lengthIndex.allMatching(2))) must contain theSameElementsAs Seq(id2 -> "14", id4 -> "35")
      await(db.run(FooRepo.lengthIndex.allMatching(3))) must be(Seq(id3 -> "275"))
      await(db.run(FooRepo.lengthIndex.find(_.largerThan(1)))) must contain theSameElementsAs Seq(id2 -> "14", id4 -> "35", id3 -> "275", id5 -> "ababa")
      await(db.run(FooRepo.lengthIndex.find(_.smallerThan(2)))) must be(Seq(id1 -> "8"))
      await(db.run(FooRepo.lengthIndex.find(_.inSet(Set(2,3))))) must contain theSameElementsAs Seq(id2 -> "14", id4 -> "35", id3 -> "275")
      await(db.run(FooRepo.lengthIndex.find(_.inSet(Set(4))))) must be(Seq())
      await(db.run(FooRepo.lengthIndex.countMatching(2))) must be (2)
      await(db.run(FooRepo.lengthIndex.countMatching(6))) must be (0)
      await(db.run(FooRepo.lengthIndex.first(_.matching(1))) )must be(Some(id1 -> "8"))
      await(db.run(FooRepo.lengthIndex.first(_.matching(19)))) must be(None)
      await(db.run(FooRepo.firstCharIndex.allMatching('8'))) must be(Seq(id1 -> "8"))
      await(db.run(FooRepo.firstCharIndex.allMatching('2'))) must be(Seq(id3 -> "275"))
      await(db.run(FooRepo.insert(id3, "279")))
      await(db.run(FooRepo.lengthIndex.allMatching(3))) must be(Seq(id3 -> "279"))
      await(db.run(FooRepo.insert(id3, "16")))
      await(db.run(FooRepo.lengthIndex.allMatching(3))) must be(Seq())
      await(db.run(FooRepo.lengthIndex.allMatching(2))) must contain theSameElementsAs Seq(id2 -> "14", id3 -> "16", id4 -> "35")
      await(db.run(FooRepo.lengthIndex.max)).value must be(5)
      await(db.run(FooRepo.insert(id3, "571115")))
      await(db.run(FooRepo.lengthIndex.max)).value must be(6)
      await(db.run(FooRepo.lengthIndex.min)).value must be(1)
      await(db.run(FooRepo.firstTwoIndex.find(_.startsWith("57")))) must be(Seq(id3 -> "571115"))
      await(db.run(FooRepo.seqIndex.allMatching('a'))) must be(Seq(id5 -> "ababa"))
  }

  "Action.seq" should "work for insert followed by get" in {
    db =>
      val id1 = FooId(UUID.randomUUID())
      val id2 = FooId(UUID.randomUUID())
      val id3 = FooId(UUID.randomUUID())
      val id4 = FooId(UUID.randomUUID())
      await(db.run(Action.seq(Seq[Action[NoStream, Any]](
        FooRepo.insert(id1, "8"),
        FooRepo(id1))
      ))) must be (Seq((), "8"))

  }

  "streaming" should "work" in {
    db =>
      val id1 = FooId(UUID.randomUUID())
      val id2 = FooId(UUID.randomUUID())
      val id3 = FooId(UUID.randomUUID())
      val id4 = FooId(UUID.randomUUID())
      await(db.run(FooRepo.insert(id1, "8")))
      await(db.run(FooRepo.insert(id2, "14")))
      await(db.run(FooRepo.insert(id3, "275")))
      await(db.run(FooRepo.insert(id4, "35")))
      awaitStream(db.stream(FooRepo.lengthIndex.allMatching(2))) must contain theSameElementsAs (Seq(id2 -> "14", id4 -> "35"))
      awaitStream(db.stream(FooRepo.allLatestEntries())) must contain theSameElementsAs (Seq(id1 -> "8", id2 -> "14", id3 -> "275", id4 -> "35"))
      await(db.run(FooRepo.insert(id3, "276")))
      awaitStream(db.stream(FooRepo.allLatestEntries())) must contain theSameElementsAs (Seq(id1 -> "8", id2 -> "14", id3 -> "276", id4 -> "35"))
      awaitStream(db.stream(FooRepo.getEntries(fromPk = 2))).map(_.id) must contain theSameElementsAs (Seq(id3, id4, id3))
  }

  "deletion" should "work" in {
    db =>
      val id1 = FooId(UUID.randomUUID())
      val id2 = FooId(UUID.randomUUID())
      val id3 = FooId(UUID.randomUUID())
      val id4 = FooId(UUID.randomUUID())
      await(db.run(FooRepo.insert(id1, "8")))
      await(db.run(FooRepo.insert(id2, "14")))
      await(db.run(FooRepo.insert(id2, "17")))
      await(db.run(FooRepo.insert(id3, "275")))
      await(db.run(FooRepo.insert(id4, "35")))
      await(db.run(FooRepo.delete(Set(id2, id3))))
      await(db.run(FooRepo.allLatestEntries())) must contain theSameElementsAs (
        Seq(id1 -> "8", id4 -> "35")
      )
      intercept[ElementNotFoundException](await(db.run(FooRepo(id2))))
      intercept[ElementNotFoundException](await(db.run(FooRepo(id3))))
      await(db.run(FooRepo(id1))) must be("8")
      await(db.run(FooRepo(id4))) must be("35")
      awaitStream(db.stream(FooRepo.lengthIndex.allMatching(2))) must contain theSameElementsAs (
        Seq(id4 -> "35"))
  }

  "lastEntry" should "return last inserted" in {
    db =>
      await(db.run(FooRepo.lastEntry())) must be(None)
      val id1 = FooId(UUID.randomUUID())
      val id2 = FooId(UUID.randomUUID())
      val id3 = FooId(UUID.randomUUID())
      val id4 = FooId(UUID.randomUUID())
      await(db.run(FooRepo.insert(id1, "8")))
      await(db.run(FooRepo.lastEntry())).value.copy(timestamp = 0) must be(EntryTableRecord(1, id1, 0, "8"))
      await(db.run(FooRepo.insert(id2, "17")))
      await(db.run(FooRepo.lastEntry())).value.copy(timestamp = 0) must be(EntryTableRecord(2, id2, 0, "17"))

  }
}
