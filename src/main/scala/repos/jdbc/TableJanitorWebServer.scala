package repos.jdbc

import java.time.Instant

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{StatusCodes, HttpResponse, ContentTypes, HttpEntity}
import akka.stream.ActorMaterializer
import repos.jdbc.MonitorActor.GetStatus
import repos.jdbc.TableJanitor.{Gap, TableJanitorStatus}
import akka.pattern.ask
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._

object TableJanitorWebServer {
  def route(monitorActor: ActorRef)(implicit ec: ExecutionContext) = {
    import akka.http.scaladsl.server.Directives._
    implicit val timeout = akka.util.Timeout(30.seconds)

    get {
      pathSingleSlash {
        complete {
          (monitorActor ? GetStatus).mapTo[Option[TableJanitorStatus]].map {
            case None =>
              Future.successful(
                HttpResponse(StatusCodes.ServiceUnavailable, entity =
                  HttpEntity(ContentTypes.`text/plain(UTF-8)`, "Not ready yet.")))
            case Some(j: TableJanitorStatus) =>
              Future.successful(
                HttpResponse(StatusCodes.OK, entity =
                  HttpEntity(ContentTypes.`text/html(UTF-8)`,
                    html.status(j).body)))
          }
        }
      }
    }
  }

  def classForState(s: TableJanitor.State) =
    if (s.gaps.nonEmpty) "red"
    else if (s.maxSeen > s.indexedAllUpTo) "yellow"
    else "green"

  def toTime(s: Long) = {
    Instant.ofEpochMilli(s)
  }
}

object WebServerTestMain extends App {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  val monitorActor = system.actorOf(Props[MonitorActor])
  monitorActor ! TableJanitorStatus(repoState =
    Map(
      "personsRepo" -> TableJanitor.State(indexedAllUpTo = 120, maxSeen = 130,
      gaps = Vector(Gap(121, 124, observed = System.currentTimeMillis()))),
      "otherRepo" -> TableJanitor.State(indexedAllUpTo = 5743, maxSeen = 5743,
        gaps = Vector.empty)),
    statusTable = Map("ix3_repo1_catchingup" -> 0L, "ix3_repo2_catchingup" -> 4L))

  Http().bindAndHandle(TableJanitorWebServer.route(monitorActor)(system.dispatcher), "localhost", 7777)
}
