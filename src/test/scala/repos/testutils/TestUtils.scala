package repos.testutils

import repos.RepoPublisher
import repos.jdbc.JdbcDb
import slick.jdbc.JdbcBackend
import slick.jdbc.JdbcBackend.Database

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

object TestUtils {
  def makeH2DB(): JdbcBackend.DatabaseDef = {
    Database.forURL(
      s"jdbc:h2:mem:test_${util.Random.nextInt};DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1",
      driver = "org.h2.Driver")
  }

  def makeH2JdbcDb(jdb: JdbcBackend.DatabaseDef): JdbcDb = {
    new JdbcDb(slick.driver.H2Driver, jdb)
  }

  def await[R](f: => Future[R]): R = Await.result(f, Duration.Inf)

  def awaitStream[T](publisher: RepoPublisher[T]): ArrayBuffer[T] = {
    val b = ArrayBuffer.empty[T]
    await(publisher.foreach(s => synchronized { b+=s }))
    b.result()
  }
}
