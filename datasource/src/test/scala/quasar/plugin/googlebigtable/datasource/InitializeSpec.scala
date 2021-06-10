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

import scala.collection.JavaConverters._

import cats.effect.IO
import cats.effect.testing.specs2.CatsIO
import cats.implicits._

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.models.{Query, Row, RowMutation}

import org.specs2.mutable.Specification

/**
  * Initializes a test instance, given that it is created and running.
  */
class InitializeSpec extends Specification with CatsIO {
  import BigTableSpecUtils._

  def ensureTable(adminClient: BigtableTableAdminClient, tableId: String, columnFamily: String): IO[Boolean] =
    IO.delay {
      if (!adminClient.exists(tableId)) {
        val req = CreateTableRequest.of(tableId).addFamily(columnFamily)
        adminClient.createTable(req)
        println(s"Table $tableId created")
        true
      } else {
        println(s"Table $tableId already exists")
        false
      }
    }

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

  def read(dataClient: BigtableDataClient, tableId: String): IO[List[Row]] =
    IO.delay {
      val query = Query.create(tableId)
      dataClient.readRows(query).iterator.asScala.toList
    }

  "initialize" >> {
    for {
      config <- readServiceAccount[IO](AuthResourceName).map(Config("precog-test", _))
      adminClient <- GoogleBigTable.adminClient[IO](config)
      dataClient <- GoogleBigTable.dataClient[IO](config)
      created <- ensureTable(adminClient, TableId, ColumnFamily)
      _ <-
        if (created) writeToTable(dataClient, TableId, ColumnFamily)
        else ().pure[IO]
      rows <- read(dataClient, TableId)
    } yield (rows.size must be_===(2))
  }
}
