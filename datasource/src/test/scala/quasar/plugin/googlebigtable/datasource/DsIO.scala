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
import cats.effect.{IO, Resource, Sync}

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.models.{Row, RowCell, RowMutation}
import com.google.protobuf.ByteString

import com.precog.googleauth.ServiceAccount
import scala.concurrent.ExecutionContext
import cats.effect.testing.specs2.CatsIO
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

trait DsIO extends CatsIO {

  implicit val ec: ExecutionContext = global

  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  def mkRowCell(cf: String, qual: String, ts: Long, value: String, labels: List[String] = List.empty): RowCell =
    RowCell.create(cf, ByteString.copyFromUtf8(qual), ts * 1000L, /*TODO support labels? labels.asJava*/ List.empty.asJava, ByteString.copyFromUtf8(value))

  val AuthResourceName = "precog-ci-275718-6d5ee6b82f02.json"
  val PrecogInstance = InstanceId("precog-test")
  val PrecogTable = TableName("test-table")

  def serviceAccount[F[_]: Sync]: F[ServiceAccount] =
    ServiceAccount.fromResourceName[F](AuthResourceName)

  lazy val runITs = serviceAccount[IO].attempt.unsafeRunSync.isRight

  def testConfig[F[_]: Sync](tableName: TableName, rowPrefix: RowPrefix): F[Config] =
    serviceAccount.map(Config(_, PrecogInstance, tableName, rowPrefix))

  def createTable(adminClient: BigtableTableAdminClient, table: TableName, columnFamilies: List[String]): IO[Unit] =
    IO {
      val req =
        columnFamilies.foldLeft(CreateTableRequest.of(table.value)) { case (rq, fam) =>
          rq.addFamily(fam)
        }
      adminClient.createTable(req)
      ()
    }

  def table(adminClient: BigtableTableAdminClient, tableName: TableName, columnFamilies: List[String], cleanup: Boolean = true): Resource[IO, Unit] =
    Resource.make(
      createTable(adminClient, tableName, columnFamilies))(
      _ => if (cleanup) IO(adminClient.deleteTable(tableName.value)) else ().pure[IO])

  def tableHarness(rowPrefix: RowPrefix, columnFamilies: List[String]): Resource[IO, (GoogleBigTableDatasource[IO], BigtableTableAdminClient, BigtableDataClient, ResourcePath, TableName)] =
    for {
      tableName <- Resource.liftF(IO(TableName(s"src_spec_${Random.alphanumeric.take(6).mkString}")))
      cfg <- Resource.liftF(testConfig[IO](tableName, rowPrefix))
      admin <- GoogleBigTable.adminClient[IO](cfg)
      data <- GoogleBigTable.dataClient[IO](cfg)
      log = Slf4jLogger.getLoggerFromName[IO]("Test")
      ds = new GoogleBigTableDatasource[IO](log, admin, data, cfg)
      _ <- table(admin, tableName, columnFamilies)
      resName = ResourceName(tableName.value + rowPrefix.resourceNamePart)
    } yield (ds, admin, data, ResourcePath.root() / resName, tableName)

  def writeToTable(dataClient: BigtableDataClient, rows: List[RowMutation]): IO[Unit] =
    rows.traverse_(row => IO(dataClient.mutateRow(row)))
}

object DsIO {
  final case class TestRow(key: String, cells: List[RowCell]) {
    lazy val toRow: Row = Row.create(ByteString.copyFromUtf8(key), cells.asJava)

    def toRowMutation(tableName: TableName): RowMutation =
      cells.foldLeft(RowMutation.create(tableName.value, key)) { case (row, cell) =>
        row.setCell(cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getValue())
      }
  }
}
