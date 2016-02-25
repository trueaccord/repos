package repos.jdbc

import akka.actor.Actor
import repos.jdbc.MonitorActor.GetStatus
import repos.jdbc.TableJanitor.TableJanitorStatus

class MonitorActor extends Actor {
  private var status: Option[TableJanitorStatus] = None

  def receive = {
    case s: TableJanitorStatus =>
      status = Some(s)
    case GetStatus =>
      sender ! status
  }
}

object MonitorActor {
  case object GetStatus
}
