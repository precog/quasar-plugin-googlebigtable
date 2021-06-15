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

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

import cats.implicits._
import cats.effect.{ContextShift, IO, Resource, Sync}

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.models.{Row, RowCell, RowMutation}
import com.google.protobuf.ByteString

import com.precog.googleauth.ServiceAccount

object BigTableSpecUtils {

  final case class TestRow(key: String, cells: List[RowCell]) {
    lazy val toRow: Row = Row.create(ByteString.copyFromUtf8(key), cells.asJava)

    def toRowMutation(tableName: TableName): RowMutation =
      cells.foldLeft(RowMutation.create(tableName.value, key)) { case (row, cell) =>
        row.setCell(cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getValue())
      }
  }

  def mkRowCell(cf: String, qual: String, ts: Long, value: String): RowCell =
    RowCell.create(cf, ByteString.copyFromUtf8(qual), ts * 1000L, List[String]().asJava, ByteString.copyFromUtf8(value))

  implicit val ioContextShift: ContextShift[IO] =
    IO.contextShift(global)

  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  val AuthResourceName = "precog-ci-275718-6d5ee6b82f02.json"
  val PrecogInstance = InstanceId("precog-test")
  val PrecogTable = TableName("test-table")

  def testConfig[F[_]: Sync](tableName: TableName, rowPrefix: RowPrefix): F[Config] =
    ServiceAccount.fromResourceName[F](AuthResourceName).map(Config(_, PrecogInstance, tableName, rowPrefix))

  def createTable(adminClient: BigtableTableAdminClient, table: TableName, columnFamilies: List[String]): IO[Unit] =
    IO {
      val req =
        columnFamilies.foldLeft(CreateTableRequest.of(table.value)) { case (rq, fam) =>
          rq.addFamily(fam)
        }
      adminClient.createTable(req)
      ()
    }

  def table(adminClient: BigtableTableAdminClient, tableName: TableName, columnFamilies: List[String]): Resource[IO, Unit] =
    Resource.make(
      createTable(adminClient, tableName, columnFamilies))(
      _ => IO(adminClient.deleteTable(tableName.value)))

  def tableHarness(rowPrefix: RowPrefix, columnFamilies: List[String]): Resource[IO, (GoogleBigTableDatasource[IO], BigtableTableAdminClient, BigtableDataClient, ResourcePath, TableName)] =
    for {
      tableName <- Resource.liftF(IO(TableName(s"src_spec_${Random.alphanumeric.take(6).mkString}")))
      cfg <- Resource.liftF(testConfig[IO](tableName, rowPrefix))
      admin <- GoogleBigTable.adminClient[IO](cfg)
      data <- GoogleBigTable.dataClient[IO](cfg)
      ds = new GoogleBigTableDatasource[IO](admin, data, cfg)
      _ <- table(admin, tableName, columnFamilies)
    } yield (ds, admin, data, ResourcePath.root() / ResourceName(tableName.value + rowPrefix.value), tableName)

  def writeToTable(dataClient: BigtableDataClient, rows: List[RowMutation]): IO[Unit] =
    rows.traverse_(row => IO(dataClient.mutateRow(row)))
}
