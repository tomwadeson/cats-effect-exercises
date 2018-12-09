package exercises

import cats.effect.concurrent.{MVar, Ref}
import cats.effect.{ExitCode, IO, IOApp, Timer}
import cats.implicits._

import scala.concurrent.duration._
import scala.util.Random

object WorkerPool extends IOApp {

  type Worker[A, B] = A => IO[B]

  def mkWorker(id: Int)(implicit timer: Timer[IO]): IO[Worker[Int, Int]] =
    Ref[IO].of(0).map { counter =>
      def simulateWork: IO[Unit] =
        IO(50 + Random.nextInt(450)).map(_.millis).flatMap(IO.sleep)

      def report: IO[Unit] =
        counter.get.flatMap(i => IO(println(s"Total processed by $id: $i")))

      x =>
        simulateWork >>
          counter.update(_ + 1) >>
          report >>
          IO.pure(x + 1)
    }

  trait WorkerPool[A, B] {
    def exec(a: A): IO[B]
  }

  object WorkerPool {

    def of[A, B](fs: List[Worker[A, B]]): IO[WorkerPool[A, B]] = {

      def adaptWorker(worker: Worker[A, B], workerChannel: MVar[IO, Worker[A, B]]): Worker[A, B] = {
        lazy val adapted: Worker[A, B] =
          a => worker(a) <* workerChannel.put(adapted).start

        adapted
      }

      for {
        workerChannel <- MVar.empty[IO, Worker[A, B]]
        _ <- fs.traverse_(worker => workerChannel.put(adaptWorker(worker, workerChannel)).start)
      } yield new WorkerPool[A, B] {
        override def exec(a: A): IO[B] =
          workerChannel.take.flatMap(worker => worker(a))
      }
    }
  }

  val testPool: IO[WorkerPool[Int, Int]] =
    List.range(0, 10)
      .traverse(mkWorker)
      .flatMap(WorkerPool.of)

  override def run(args: List[String]): IO[ExitCode] =
    for {
      workerPool <- testPool
      _ <- List.range(0, 100).parTraverse_(workerPool.exec)
    } yield ExitCode.Success
}
