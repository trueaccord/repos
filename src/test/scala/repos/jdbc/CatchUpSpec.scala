package repos.jdbc

import java.util.UUID

import akka.actor.{PoisonPill, Props, ActorSystem}
import akka.stream.ActorMaterializer
import akka.testkit.TestProbe
import org.scalatest.{Inside, LoneElement, MustMatchers}
import repos.{Database, EntryTableRecord}
import repos.jdbc.TableJanitor.{Gap, State}
import repos.testutils.TestUtils._
import repos.testutils.{FooId, FooRepo, TestUtils}

import scala.concurrent.ExecutionContext.Implicits.global

class CatchUpSpec extends org.scalatest.fixture.WordSpec with MustMatchers with LoneElement with Inside {
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
    val id8 = FooId(UUID.randomUUID())
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

    def insertNoIndex(on: JdbcDb)(entries: (FooId, String)*) = {
      await(
        on.innerRepo(FooRepo).insert(entries, skipIndexesForTesting = true))
    }

    def createTable(on: JdbcDb = db) = {
      import on.profile.api._
      on.jc.blockingWrapper(on.jc.JanitorIndexStatus.schema.create)
      await(on.run(FooRepo.create()))
      insertNoIndex(on)(id1 -> d1, id2 -> d2)
      await(on.run(FooRepo.insert(id3 -> d3, id4 -> d4)))
      insertNoIndex(on)(id5 -> d5, id6 -> d6)
      await(on.run(FooRepo.insert(id7 -> d7)))
    }
  }

  // We use additional databases to create the illusion of missing pks showing up later.

  def withNewDb[T](t: JdbcDb => T): T = {
    val h2Other = TestUtils.makeH2DB()
    val dbOther = TestUtils.makeH2JdbcDb(h2Other)
    try {
      t(dbOther)
    } finally {
      TestUtils.await(h2Other.shutdown)
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

  "catchupForRepo" must {
    "be able to catch all indexes from scratch" in {
      t =>
        import t._
        t.createTable()
        val statusTable = Map.empty[String, Long].withDefaultValue(-1L)

        val r = TableJanitor.catchUpForRepo(db, db, System.currentTimeMillis(), statusTable, FooRepo,
          State(0, 0, Vector.empty))
        r must be(State(7, 7, Vector.empty))
        val newStatus = TableJanitor.loadJanitorIndexStatus(db)
        newStatus must be(Map(
          "ix3_foo__text_text" -> 7, "ix3_foo__len_index" -> 7, "ix3_foo__first_ch" -> 7, "ix3_foo__first_two_ch" -> 7
        ))
    }

    "report gaps, fill them, and discard them" in {
      t =>
        withNewDb { db2 =>
          import t._
          t.createTable(on = db)
          await(db.run(FooRepo.delete(Set(id2, id3, id6))))
          val statusTable = Map.empty[String, Long].withDefaultValue(-1L)

          val newState = TableJanitor.catchUpForRepo(db, db, System.currentTimeMillis(), statusTable, FooRepo,
            State(0, 0, Vector.empty))
          inside(newState) {
            case State(1, 7, v) =>
              inside(v) {
                case Vector(Gap(2, 3, _), Gap(6, 6, _)) =>
              }
          }
          val statusTable2 = TableJanitor.loadJanitorIndexStatus(db)
          statusTable2 must be(Map(
            "ix3_foo__text_text" -> 1, "ix3_foo__len_index" -> 1, "ix3_foo__first_ch" -> 1,
            "ix3_foo__first_two_ch" -> 1
          ))

          t.createTable(on = db2)
          await(db2.run(FooRepo.delete(Set(id3))))
          await(db2.run(FooRepo.insert(id8 -> "8")))

          val newState2 = TableJanitor.catchUpForRepo(db2, db2,
            System.currentTimeMillis(), statusTable2, FooRepo,
            newState)
          inside(newState2) {
            case State(2, 8, v) =>
              inside(v) {
                case Vector(Gap(3, 3, _)) =>
              }
          }

          val statusTable3 = TableJanitor.loadJanitorIndexStatus(db2)
          statusTable3 must be(Map(
            "ix3_foo__text_text" -> 2, "ix3_foo__len_index" -> 2, "ix3_foo__first_ch" -> 2,
            "ix3_foo__first_two_ch" -> 2
          ))

          val newState3 = TableJanitor.catchUpForRepo(db2, db2,
            System.currentTimeMillis() + TableJanitor.FORGET_MISSING_AFTER_MS * 2, statusTable2, FooRepo,
            newState2)

          val statusTable4 = TableJanitor.loadJanitorIndexStatus(db2)
          statusTable4 must be(Map(
            "ix3_foo__text_text" -> 8, "ix3_foo__len_index" -> 8, "ix3_foo__first_ch" -> 8,
            "ix3_foo__first_two_ch" -> 8
          ))
        }
    }

    "can catch up a new index, not moving forward old indexes" in {
      t =>
        import t._
        t.createTable()

        val statusTable = Map(
          "ix3_foo__text_text" -> -1L, "ix3_foo__len_index" -> -1L, "ix3_foo__first_ch" -> 1000L,
          "ix3_foo__first_two_ch" -> 1000L)
        statusTable.foreach {
          t => db.jc.JanitorIndexStatus.updateLastPkForIndex(t._1, t._2)
        }
        await(db.run(FooRepo.textIndex.allMatching(d5))) must not contain (id5 -> d5)

        val newState = TableJanitor.catchUpForRepo(db, db, System.currentTimeMillis(), statusTable, FooRepo,
          State(0, 0, Vector.empty))

        val statusTable2 = TableJanitor.loadJanitorIndexStatus(db)
        statusTable2 must be(Map(
          "ix3_foo__text_text" -> 7, "ix3_foo__len_index" -> 7, "ix3_foo__first_ch" -> 1000,
          "ix3_foo__first_two_ch" -> 1000
        ))
        await(db.run(FooRepo.textIndex.allMatching(d5))) must contain (id5 -> d5)
    }
}
}
