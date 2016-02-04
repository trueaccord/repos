package repos.jdbc

import java.util.UUID

import akka.actor.{PoisonPill, Props, ActorSystem}
import akka.stream.ActorMaterializer
import akka.testkit.TestProbe
import org.scalatest.{LoneElement, MustMatchers}
import repos.EntryTableRecord
import repos.testutils.TestUtils._
import repos.testutils.{FooId, FooRepo, TestUtils}

import scala.concurrent.ExecutionContext.Implicits.global

class CatchUpSpec extends org.scalatest.fixture.WordSpec with MustMatchers with LoneElement {
  spec =>

  type FixtureParam = CatchUpTest

  class CatchUpTest {
    val id1 = FooId(UUID.randomUUID())
    val id2 = FooId(UUID.randomUUID())
    val id3 = FooId(UUID.randomUUID())
    val id4 = FooId(UUID.randomUUID())
    val id5 = FooId(UUID.randomUUID())
    val id6 = FooId(UUID.randomUUID())
    val id7 = FooId(UUID.randomUUID())
    val d1 = "123"
    val d2 = "12345"
    val d3 = "abcde"
    val d4 = "12345678"
    val d5 = "!@#$%^&*"
    val d6 = "ABCDEFGH"
    val d7 = "Foo"
    val h2 = TestUtils.makeH2DB()
    val db = TestUtils.makeH2JdbcDb(h2)

    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    def withTestProbe[T](code: TestProbe => T) = {
      val testProbe = TestProbe()(system)
      try {
        code(testProbe)
      } finally {
        testProbe.ref ! PoisonPill
      }
    }

    def insertNoIndex(entries: (FooId, String)*) = {
      await(
        db.innerRepo(FooRepo).insert(entries, skipIndexesForTesting = true))
    }

    def unindexedEntries(startPk: Long) = {
      db.jc.blockingWrapper(
        TableJanitor.unindexedEntries(db)(FooRepo, startPk)(FooRepo.lengthIndex))
    }

    def createTable() = {
      await(db.run(FooRepo.create()))
      insertNoIndex(id1 -> d1, id2 -> d2)
      await(db.run(FooRepo.insert(id3 -> d3, id4 -> d4)))
      insertNoIndex(id5 -> d5, id6 -> d6)
      await(db.run(FooRepo.insert(id7 -> d7)))
    }
  }

  override def withFixture(test: OneArgTest) = {
    val t = new CatchUpTest
    try {
      test(t)
    } finally {
      t.materializer.shutdown()
      TestUtils.await(t.system.terminate())
      TestUtils.await(t.h2.shutdown)
    }

  }

  "CatchUpTest" must {
    "insert into testRepo" in {
      t =>
        import t._
        t.createTable()
        await(db.run(FooRepo(id1))) must be(d1)
    }

    "not insert into the indexes" in {
      t =>
        import t._
        t.createTable()
        await(db.run(FooRepo.lengthIndex.count(_.largerThan(0)))) must be(3)
        await(db.run(FooRepo.textIndex.countMatching("123"))) must be(0)
    }
  }

  "unindexedEntries" must {
    "find all unindexed entries" in {
      t =>
        import t._
        t.createTable()
        val entries: Seq[EntryTableRecord[FooId, String]] = unindexedEntries(0)
        entries.map(_.entry) must be(Seq(d1, d2, d5, d6))

        unindexedEntries(entries(1).pk).map(_.entry) must be(Seq(d5, d6))
    }

    "detect unindexed entry that has same id of an existing one" in {
      t =>
        import t._
        t.createTable()
        // d3 is indexed
        await(db.run(FooRepo.textIndex.allMatching(d3))).loneElement._2 must be(d3)
        unindexedEntries(0).map(_.entry) must not contain (d3)

        insertNoIndex(id3 -> d3)
        unindexedEntries(0).map(_.entry) must contain(d3)
    }
  }

  "indexEntries" must {
    "index all unindexed entries" in {
      t =>
        import t._
        RepoManagement.createMissingRepos(t.db, Seq.empty)
        t.createTable()
        val unindexed = Seq(d1, d2, d5, d6)
        unindexed.foreach(i => await(db.run(FooRepo.textIndex.countMatching(i))) > 0 must be(false))
        TableJanitor.indexEntries(db)(FooRepo, 0)(FooRepo.textIndex)
        unindexed.foreach(i => await(db.run(FooRepo.textIndex.countMatching(i))) > 0 must be(true))
    }
  }

  "tableJanitor" must {
    "index on tick" in {
      t =>
        import t._
        withTestProbe {
          testProbe =>
            // Wait for actor to boot
            val janitor = system.actorOf(
              Props(classOf[TableJanitor], db, Seq(FooRepo), materializer))

            testProbe.send(janitor, TableJanitor.Tick)
            testProbe.expectMsg(TableJanitor.Ok)

            val id1 = FooId(UUID.randomUUID())
            val id2 = FooId(UUID.randomUUID())
            insertNoIndex(id1 -> "foo", id2 -> "bar")

            await(db.run(FooRepo.textIndex.allMatching("foo"))) mustBe 'empty
            testProbe.send(janitor, TableJanitor.Tick)
            testProbe.expectMsg(TableJanitor.Ok)
            await(db.run(FooRepo.textIndex.allMatching("foo"))).loneElement._2 must be("foo")
            janitor ! PoisonPill
        }
    }
  }
}
