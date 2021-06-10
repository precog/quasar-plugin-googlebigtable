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

import quasar.api.datasource.DatasourceType
import quasar.api.resource._, ResourcePath._
import quasar.connector._
import quasar.connector.datasource.{BatchLoader, LightweightDatasource, Loader}
import quasar.qscript.InterpretedRead

import scala.collection.JavaConverters._

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._

import com.google.bigtable.admin.v2.ListTablesRequest
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.config.{BigtableOptions, CredentialOptions}, CredentialOptions.UserSuppliedCredentialOptions
import com.google.cloud.bigtable.grpc.BigtableSession

import fs2.Stream

import shims.equalToCats

final class GoogleBigTableDatasource[F[_]: Sync: MonadResourceErr](
    adminClient: BigtableTableAdminClient,
    client: BigtableSession,
    config: Config)
    extends LightweightDatasource[Resource[F, ?], Stream[F, ?], QueryResult[F]] {

  def kind: DatasourceType = GoogleBigTableDatasource.DsType

  def loaders: NonEmptyList[Loader[Resource[F,*], InterpretedRead[ResourcePath], QueryResult[F]]] =
    NonEmptyList.of(Loader.Batch(BatchLoader.Full { (iRead: InterpretedRead[ResourcePath]) =>
      iRead.path match {
      case ResourcePath.Root => ???
      case ResourcePath.Leaf(file) => ???
    }
  }))

  def pathIsResource(path: ResourcePath): Resource[F, Boolean] = path match {
    case ResourcePath.Root => false.pure[Resource[F, *]]
    case t /: ResourcePath.Root => Resource.liftF(Sync[F].delay(adminClient.exists(t)))
    case _ => false.pure[Resource[F, *]]
  }

  def prefixedChildPaths(prefixPath: ResourcePath)
      : Resource[F, Option[Stream[F, (ResourceName, ResourcePathType.Physical)]]] = {
    if (prefixPath === ResourcePath.Root)
      Resource.liftF {
        fetchTables(config.instancePath).map(r => (r, ResourcePathType.leafResource))
          .some
          .pure[F]
      }
    else
      none.pure[Resource[F, *]]
  }

  private def fetchTables(instancePath: String): Stream[F, ResourceName] = {
    val req =
      ListTablesRequest
        .newBuilder()
        .setParent(instancePath)
        .build()

    val ts = Sync[F].delay {
      client
        .getTableAdminClient()
        .listTables(req)
        .getTablesList
        .asScala
        .flatMap(t => config.tableNamePrism.getOption(t.getName).fold(List.empty[ResourceName])(t => List(ResourceName(t.value))))
    }
    Stream.evalSeq(ts)
  }
}

object GoogleBigTableDatasource {

  val DsType: DatasourceType = DatasourceType("googlebigtable", 1L)

  def mkSess[F[_]: Sync](config: Config): Resource[F, BigtableSession] = Resource.make(
    for {
      creds <- config.credentials
      sess <- Sync[F].delay {
        val opts =
          BigtableOptions
            .builder()
            .setCredentialOptions(new UserSuppliedCredentialOptions(creds))
            .setProjectId(config.serviceAccount.projectId)
            .setInstanceId(config.instanceId)
            .setUserAgent("Precog")
            .build()
        new BigtableSession(opts)
      }
    } yield sess)(sess => Sync[F].delay(sess.close()))

  def apply[F[_]: Sync: MonadResourceErr](
      config: Config): Resource[F, GoogleBigTableDatasource[F]] = {
    for {
      adminClient <- Resource.liftF(GoogleBigTable.adminClient(config))
      sess <- mkSess(config)
     } yield new GoogleBigTableDatasource(adminClient, sess, config)
  }
}
