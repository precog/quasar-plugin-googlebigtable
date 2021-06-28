/*
 * Copyright 2020 Precog Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.plugin.googlebigtable.datasource

import slamdata.Predef._

import cats.effect.{ConcurrentEffect, Effect, IO}
import fs2.Stream
import fs2.concurrent.Queue

object CallbackHandler {

  type Callback[F[_], A] = Either[Throwable, Option[F[A]]] => Unit
  type Handler[F[_], A] = Callback[F, A] => F[Unit]

  def toStream[F[_]: ConcurrentEffect, A](
      handler: Handler[F, A],
      maxQueueSize: Int)
      : Stream[F, A] =
    (for {
      q <- Stream.eval(Queue.bounded[F, Either[Throwable, Option[F[A]]]](maxQueueSize))
      _ <- Stream.eval(handler(enqueueEvent(q)))
      a <- q.dequeue.rethrow.unNoneTerminate
    } yield Stream.eval(a)).flatten

  private def enqueueEvent[F[_]: Effect, A](q: Queue[F, A])(event: A): Unit =
    Effect[F].runAsync(q.enqueue1(event))(_ => IO.unit).unsafeRunSync

}
