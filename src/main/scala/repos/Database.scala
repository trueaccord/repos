package repos

import java.util.concurrent.atomic.AtomicReferenceArray

import org.reactivestreams.{Publisher, Subscriber}
import repos.Action._

import scala.collection.mutable
import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Failure, Success}

trait Database {
  class DatabaseActionContext extends Context {
    override def isStreaming: Boolean = false
  }

  class StreamingDatabaseActionContext[R] extends Context {
    private[repos] var publisher: Publisher[R] = _
    override def isStreaming: Boolean = true
  }
  sealed trait Context {
    def isStreaming: Boolean
  }

  object Context {
    def go[R, A](ctx: Context)(run: => Future[A])(stream: => Publisher[R]): Future[A] = ctx match {
      case c: StreamingDatabaseActionContext[R] @unchecked =>
        c.publisher = stream
        Future.successful(null.asInstanceOf[A])
      case _ =>
        run
    }
  }

  def runRepoAction[S <: NoStream, Id, M, R](e: RepoAction[S, Id, M, R], ctx: Context)(implicit ec: ExecutionContext): Future[R]

  def runInContext[S <: NoStream, R](a: Action[S, R], ctx: Context)(implicit ec: ExecutionContext): Future[R] = {
    a match {
      case FlatMapAction(source, f) =>
        runInContext(source, ctx).flatMap(r => runInContext(f(r), ctx))
      case MappedAction(source, f) =>
        runInContext(source, ctx).map(f)
      case AndThenAction(actions) =>
        actions.tail.foldLeft(runInContext(actions.head, ctx)) { (f, nextAction) => f.flatMap(_ => runInContext(nextAction, ctx)) }.asInstanceOf[Future[R]]
      case ActionSeq(actions) =>
        assert(actions.nonEmpty)
        val len = actions.length
        val results = new AtomicReferenceArray[Any](len)
        def run(pos: Int): Future[Any] = {
          if (pos == len) Future.successful {
            val b = mutable.IndexedSeq.newBuilder[Any]
            var i = 0
            while (i < len) {
              b += results.get(i)
              i += 1
            }
            b.result()
          }
          else runInContext(actions(pos), ctx).flatMap { (v: Any) =>
            results.set(pos, v)
            run(pos + 1)
          }
        }
        run(0).asInstanceOf[Future[R]]
      case Tuple2Action(a, b) =>
        runInContext(a, ctx) flatMap { ar => runInContext(b, ctx).map((ar, _)) }
      case e: RepoAction[_, _, _, _] =>
        runRepoAction(e, ctx)
    }
  }

  def run[S <: NoStream, R](a: Action[S, R])(implicit ec: ExecutionContext) = {
    runInContext(a, new DatabaseActionContext)
  }

  def stream[A, R](a: Action[Streaming[R], A])(implicit ec: ExecutionContext): RepoPublisher[R] = {
    new RepoPublisher[R] {
      override def subscribe(s: Subscriber[_ >: R]): Unit = {
        val ctx = new StreamingDatabaseActionContext[R]
        runInContext(a, ctx)
          .onComplete {
            case f: Success[A] =>
              ctx.publisher.subscribe(s)
            case Failure(th) =>
              s.onError(th)
          }
      }
    }
  }

  /** Finds the last pk the scanner has checkpointed for (jobName, repo) */
  def scannerFindLastPk[Id, M](jobName: String, repo: Repo[Id, M]): Future[Option[Long]]

  /** Update the last pk the scanner has reached. */
  def scannerUpdateCheckpoint[Id, M](jobName: String, repo: Repo[Id, M], lastPk: Long): Future[Unit]
}
