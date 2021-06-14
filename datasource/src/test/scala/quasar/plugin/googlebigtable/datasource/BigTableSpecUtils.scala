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

import quasar.api.resource.{ResourceName, ResourcePath}
import quasar.connector.ResourceError
import quasar.contrib.scalaz.MonadError_

import scala.util.Random

import cats.implicits._
import cats.effect.{IO, Resource, Sync}

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.models.RowMutation

import com.precog.googleauth.ServiceAccount

object BigTableSpecUtils {
  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  val AuthResourceName = "precog-ci-275718-6d5ee6b82f02.json"
  val InstanceName = "precog-test"
  val TableId = "test-table"
  val ColumnFamily = "cf1"

  def testConfig[F[_]: Sync]: F[Config] =
    ServiceAccount.fromResourceName[F](AuthResourceName).map(Config(InstanceName, _))

  def ensureTable(adminClient: BigtableTableAdminClient, tableId: String, columnFamily: String): IO[Boolean] =
    IO {
      if (!adminClient.exists(tableId)) {
        val req = CreateTableRequest.of(tableId).addFamily(columnFamily)
        adminClient.createTable(req)
        true
      } else {
        false
      }
    }

  def table(adminClient: BigtableTableAdminClient): Resource[IO, (ResourcePath, String)] = {
    val acq =
      for {
        tableName <- IO(s"src_spec_${Random.alphanumeric.take(6).mkString}")
        fam = "cf1"
        _ <- ensureTable(adminClient, tableName, fam)
      } yield tableName

    Resource
      .make(acq)(name => IO(adminClient.deleteTable(name)))
      .map(n => (ResourcePath.root() / ResourceName(n), n))
  }

  def tableHarness(): Resource[IO, (GoogleBigTableDatasource[IO], BigtableTableAdminClient, BigtableDataClient, ResourcePath, String)] =
    for {
      cfg <- Resource.liftF(testConfig[IO])
      admin <- GoogleBigTable.adminClient[IO](cfg)
      data <- GoogleBigTable.dataClient[IO](cfg)
      ds = new GoogleBigTableDatasource[IO](admin, data, cfg)
      (path, name) <- table(admin)
    } yield (ds, admin, data, path, name)

  def writeToTable(dataClient: BigtableDataClient, tableId: String, columnFamily: String): IO[Unit] =
    List("World", "Joe").zipWithIndex.traverse_ { p =>

      val row =
        RowMutation.create(tableId, s"rowKey${p._2}")
          .setCell(columnFamily, "name", p._1)
          .setCell(columnFamily, "greeting", s"Hey ${p._1}!")
      IO.delay {
        dataClient.mutateRow(row)
      }
    }
}
