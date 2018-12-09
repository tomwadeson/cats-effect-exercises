package exercises

import cats.effect.concurrent.{MVar, Ref}
import cats.effect._
import cats.implicits._
import cats.effect.implicits._

import scala.concurrent.duration._
import scala.util.Random

object WorkerPool extends IOApp {

  type Worker[F[_], A, B] = A => F[B]

  def mkWorker[F[_]](id: Int)(implicit F: Sync[F], timer: Timer[F]): F[Worker[F, Int, Int]] =
    Ref[F].of(0).map { counter =>
      def simulateWork: F[Unit] =
        F.delay(50 + Random.nextInt(450)).map(_.millis).flatMap(timer.sleep)

      def report: F[Unit] =
        counter.get.flatMap(i => F.delay(println(s"Total processed by $id: $i")))

      x =>
        simulateWork >>
          counter.update(_ + 1) >>
          report >>
          F.pure(x + 1)
    }

  trait WorkerPool[F[_], A, B] {
    def exec(a: A): F[B]
  }

  object WorkerPool {

    def of[F[_] : Concurrent, A, B](workers: List[Worker[F, A, B]]): F[WorkerPool[F, A, B]] = {

      def adaptWorker(worker: Worker[F, A, B], workerChannel: MVar[F, Worker[F, A, B]]): Worker[F, A, B] = {
        lazy val adapted: Worker[F, A, B] =
          a => worker(a) <* workerChannel.put(adapted).start

        adapted
      }

      for {
        workerChannel <- MVar.empty[F, Worker[F, A, B]]
        _ <- workers.traverse_(worker => workerChannel.put(adaptWorker(worker, workerChannel)).start)
      } yield new WorkerPool[F, A, B] {
        override def exec(a: A): F[B] =
          workerChannel.take.flatMap(worker => worker(a))
      }
    }
  }

  val testPool: IO[WorkerPool[IO, Int, Int]] =
    List.range(0, 10)
      .traverse(mkWorker[IO])
      .flatMap(WorkerPool.of[IO, Int, Int])

  override def run(args: List[String]): IO[ExitCode] =
    for {
      workerPool <- testPool
      _ <- List.range(0, 50).parTraverse_(workerPool.exec)
    } yield ExitCode.Success
}
