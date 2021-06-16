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

import cats.effect.{IO, Resource}

import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.models.{Query => GQuery, Row, RowMutation}

import org.specs2.mutable.Specification

/**
  * Initializes a test instance, given that it is created and running.
  */
class InitializeSpec extends Specification with DsIO {

  def read(dataClient: BigtableDataClient, table: TableName): IO[List[Row]] =
    IO.delay {
      val query = GQuery.create(table.value)
      dataClient.readRows(query).iterator.asScala.toList
    }

  "initialize" >> {
    val tbl = TableName("some-table")
    val in = List(
      RowMutation.create(tbl.value, "rowKey1")
        .setCell("cf1", "name", "Joe")
        .setCell("cf1", "greeting", "Hey Joe!"),
      RowMutation.create(tbl.value, "rowKey2")
        .setCell("cf1", "name", "World")
        .setCell("cf1", "greeting", "Hello World!"))

    for {
      config <- Resource.liftF(testConfig[IO](tbl, RowPrefix("")))
      adminClient <- GoogleBigTable.adminClient[IO](config)
      dataClient <- GoogleBigTable.dataClient[IO](config)
      _ <- table(adminClient, tbl, List("cf1"))
      _ <- Resource.liftF(writeToTable(dataClient, in))
      out <- Resource.liftF(read(dataClient, tbl))
    } yield (out.size must be_===(2))
  }
}
