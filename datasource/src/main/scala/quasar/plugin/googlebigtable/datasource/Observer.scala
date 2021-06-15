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

import com.google.api.gax.rpc.{StateCheckingResponseObserver, StreamController}
import com.google.cloud.bigtable.data.v2.models.Row
import cats.effect.Sync

class Observer[F[_]: Sync](callback: Either[Throwable, Option[F[Row]]] => Unit) extends StateCheckingResponseObserver[Row] {

  override protected def onStartImpl(controller: StreamController): Unit =
    ()

  override protected def onResponseImpl(row: Row): Unit =
    callback(Right(Some(Sync[F].delay(row))))

  override protected def onErrorImpl(t: Throwable): Unit =
    callback(Left(t))

  override protected def onCompleteImpl(): Unit =
    callback(Right(None))

}

object Observer {
  def handler[F[_]: Sync](f: Observer[F] => Unit): (Either[Throwable, Option[F[Row]]] => Unit) => F[Unit] = { cb =>
    Sync[F].delay {
      f(new Observer(cb))
    }
  }
}
