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
