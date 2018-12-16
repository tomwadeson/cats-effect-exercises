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
        F.delay(50 + Random.nextInt(450))
          .flatMap { i =>
            if ((i & 3) == 3) F.raiseError[Int](new Exception) else F.pure(i)
          }
          .map(_.millis)
          .flatMap(timer.sleep)

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
    def addWorker(worker: Worker[F, A, B]): F[Unit]
    def removeAllWorkers: F[Unit]
  }

  object WorkerPool {

    def of[F[_]: Concurrent, A, B](workers: List[Worker[F, A, B]]): F[WorkerPool[F, A, B]] = {

      def adaptWorker(worker: Worker[F, A, B], workerChannel: MVar[F, Worker[F, A, B]]): Worker[F, A, B] = {

        lazy val adapted: Worker[F, A, B] =
          a => worker(a).guarantee(addWorkerInternal(adapted, workerChannel))

        adapted
      }

      def addWorkerInternal(worker: Worker[F, A, B], workerChannel: MVar[F, Worker[F, A, B]]): F[Unit] =
        workerChannel.put(worker).start.void

      for {
        workerChannel    <- MVar.empty[F, Worker[F, A, B]]
        workerChannelRef <- Ref[F].of(workerChannel)
        _                <- workers.traverse_(w => addWorkerInternal(adaptWorker(w, workerChannel), workerChannel))
      } yield
        new WorkerPool[F, A, B] {

          override def exec(a: A): F[B] =
            for {
              workerChannel <- workerChannelRef.get
              worker        <- workerChannel.take
              b             <- worker(a)
            } yield b

          override def addWorker(worker: Worker[F, A, B]): F[Unit] =
            for {
              workerChannel <- workerChannelRef.get
              _             <- addWorkerInternal(adaptWorker(worker, workerChannel), workerChannel)
            } yield ()

          override def removeAllWorkers: F[Unit] =
            for {
              newWorkerChannel <- MVar.empty[F, Worker[F, A, B]]
              _                <- workerChannelRef.set(newWorkerChannel)
            } yield ()
        }
    }
  }

  val testPool: IO[WorkerPool[IO, Int, Int]] =
    List
      .range(0, 3)
      .traverse(mkWorker[IO])
      .flatMap(WorkerPool.of[IO, Int, Int])

  override def run(args: List[String]): IO[ExitCode] =
    for {
      workerPool <- testPool
      _          <- workerPool.exec(1).attempt
      _          <- workerPool.exec(2).attempt
      _          <- workerPool.exec(3).attempt
      _ = println("Removing all workers")
      _ <- workerPool.removeAllWorkers
      f <- List.range(0, 20).traverse_(workerPool.exec(_).attempt).start
      _ <- IO.sleep(2.seconds)
      _ = println("Adding a worker")
      newWorker1 <- mkWorker[IO](100)
      _          <- workerPool.addWorker(newWorker1)
      _          <- f.join
    } yield ExitCode.Success
}
