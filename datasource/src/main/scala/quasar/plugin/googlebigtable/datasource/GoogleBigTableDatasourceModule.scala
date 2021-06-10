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

import quasar.RateLimiting
import quasar.api.datasource.{DatasourceError, DatasourceType}
import quasar.connector._
import quasar.connector.datasource.{LightweightDatasourceModule, Reconfiguration}
import quasar.plugin.googlebigtable.datasource.json._

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.util.Either

import argonaut._, Argonaut._
import cats.effect._
import cats.kernel.Hash
import cats.implicits._
import scalaz.NonEmptyList

import scala.Predef.???

object GoogleBigTableDatasourceModule extends LightweightDatasourceModule {

  override def kind: DatasourceType = DatasourceType("googlebigtable", 1L)

  override def sanitizeConfig(config: Json): Json = config.as[Config].result match {
    case Left(_) => config
    case Right(cfg) => cfg.sanitize.asJson
  }

  override def migrateConfig[F[_]: Sync](from: Long, to: Long, config: Json): F[Either[DatasourceError.ConfigurationError[Json],Json]] =
    Sync[F].pure(Right(config))

  override def reconfigure(original: Json, patch: Json): Either[DatasourceError.ConfigurationError[Json],(Reconfiguration, Json)] = ???

  def lightweightDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer, A: Hash](
      config: Json,
      rateLimiting: RateLimiting[F,A],
      byteStore: ByteStore[F],
      auth: UUID => F[Option[ExternalCredentials[F]]])(
      implicit ec: ExecutionContext)
      : Resource[F,Either[DatasourceError.InitializationError[Json],LightweightDatasourceModule.DS[F]]] =
    config.as[Config].result match {
      case Right(cfg) => ???

      case Left((msg, _)) =>
        DatasourceError
          .invalidConfiguration[Json, DatasourceError.InitializationError[Json]](kind, sanitizeConfig(config), NonEmptyList(msg))
          .asLeft[LightweightDatasourceModule.DS[F]]
          .pure[Resource[F, ?]]
    }
}
