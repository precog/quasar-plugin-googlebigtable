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

import quasar.api.DataPathSegment
import quasar.api.datasource.DatasourceType
import quasar.api.push.{InternalKey, OffsetPath}
import quasar.api.resource._, ResourcePath._
import quasar.common.data.{QDataRValue, RValue}
import quasar.connector._
import quasar.connector.datasource.{BatchLoader, LightweightDatasource, Loader}
import quasar.qscript.InterpretedRead

import cats.Applicative
import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.data.v2.BigtableDataClient

import fs2.Stream
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import shims.equalToCats
import skolems.∃

final class GoogleBigTableDatasource[F[_]: ConcurrentEffect: MonadResourceErr](
    log: SelfAwareStructuredLogger[F],
    adminClient: BigtableTableAdminClient,
    dataClient: BigtableDataClient,
    config: Config)
    extends LightweightDatasource[Resource[F, ?], Stream[F, ?], QueryResult[F]] {

  def kind: DatasourceType = GoogleBigTableDatasource.DsType

  val loaders: NonEmptyList[Loader[Resource[F,*], InterpretedRead[ResourcePath], QueryResult[F]]] =
    NonEmptyList.of(Loader.Batch(BatchLoader.Seek(loader(_, _))))

  def pathIsResource(path: ResourcePath): Resource[F, Boolean] =
    (config.resourcePath === path).pure[Resource[F, *]]

  type CPS = Stream[F, (ResourceName, ResourcePathType.Physical)]

  def prefixedChildPaths(prefixPath: ResourcePath)
      : Resource[F, Option[CPS]] = {
    val res: Option[CPS] =
      if (prefixPath === ResourcePath.Root)
        Stream.emit((config.resourceName, ResourcePathType.leafResource)).some
      else if (prefixPath === config.resourcePath)
        (Stream.empty: CPS).some
      else
        none

    res.pure[Resource[F, *]]
  }

  private def loader(iRead: InterpretedRead[ResourcePath], offset: Option[Offset]):
      Resource[F, QueryResult[F]] = {
    val path = iRead.path
    val errored =
      MonadResourceErr.raiseError(ResourceError.pathNotFound(path))

    val rows: Stream[F, RValue] =
      if (path === config.resourcePath)
        for {
          off <- Stream.eval(offset.traverse(mkOffset(path, _)))
          query = Query(config.tableName, config.rowPrefix, off).googleQuery
          _ <- Stream.eval(log.debug(s"Executing query: $query"))
          res <- Evaluator[F](dataClient, query, Evaluator.DefaultMaxQueueSize).evaluate
        } yield res
      else
        Stream.eval(errored)

    QueryResult.parsed(QDataRValue, ResultData.Continuous(rows), iRead.stages).pure[Resource[F, *]]
  }

  private def mkOffset(resourcePath: ResourcePath, offset: Offset): F[∃[InternalKey.Actual]] = {
    def ensurePath(path: OffsetPath): F[Unit] =
      path match {
        case NonEmptyList(DataPathSegment.Field("key"), List()) =>
          ().pure[F]
        case p =>
          val s = p.map(_.show).mkString_("")
          MonadResourceErr.raiseError(ResourceError.seekFailed(
              resourcePath,
              s"Unsupported offset path '$s'"))
      }

    for {
      internalOffset <- offset match {
        case internal: Offset.Internal => internal.pure[F]
        case _ =>
          MonadResourceErr.raiseError[Offset.Internal](ResourceError.seekFailed(
            resourcePath,
            "External offsets are not supported"))
      }
      _ <- ensurePath(internalOffset.path)
    } yield (internalOffset.value)
  }

}

object GoogleBigTableDatasource {

  val DsType: DatasourceType = DatasourceType("googlebigtable", 1L)

  def apply[F[_]: ConcurrentEffect: MonadResourceErr](
      log: SelfAwareStructuredLogger[F],
      config: Config)
      : Resource[F, GoogleBigTableDatasource[F]] =
    Applicative[Resource[F, *]].map2(
      GoogleBigTable.adminClient(config), GoogleBigTable.dataClient(config))(
      new GoogleBigTableDatasource(log, _, _, config))
}
