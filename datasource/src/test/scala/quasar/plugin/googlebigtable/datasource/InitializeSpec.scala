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
import com.google.cloud.bigtable.data.v2.models.{Query => GQuery, Row}

import org.specs2.mutable.Specification

/**
  * Initializes a test instance, given that it is created and running.
  */
class InitializeSpec extends Specification with DsIO {

  import DsIO._

  skipAllIf(!runITs)

  // whether the table gets dropped after execution
  val Cleanup = true

  def read(dataClient: BigtableDataClient, table: TableName): IO[List[Row]] =
    IO.delay {
      val query = GQuery.create(table.value)
      dataClient.readRows(query).iterator.asScala.toList
    }

  "initialize" >> {
    val tbl = TableName("some-table")

    def mkRow(key: String, i: Int) = TestRow(f"$key#$i%05d", List(
      mkRowCell("cf1", "a", 1L, key + "v1"),
      mkRowCell("cf1", "b", 2L, key + "v2"),
      mkRowCell("cf2", "c", 3L, key + "v3"),
      mkRowCell("cf2", "d", 4L, key + "v4"),
      mkRowCell("cf2", "e", 5L, key + "v5")))

    def mkRows(rows: Int, start: Int) =
      List.range(start, rows + start).map(mkRow("before", _)) ++
        List.range(start, rows + start).map(mkRow("zafter", _)) ++
          List.range(start, rows + start).map(mkRow("row", _))

    val nr = 3
    val in = mkRows(nr, 0).map(_.toRowMutation(tbl))

    for {
      config <- Resource.eval(testConfig[IO](tbl, RowPrefix("")))
      adminClient <- GoogleBigTable.adminClient[IO](config)
      dataClient <- GoogleBigTable.dataClient[IO](config)
      _ <- table(adminClient, tbl, List("cf1", "cf2"), cleanup = Cleanup)
      _ <- Resource.eval(writeToTable(dataClient, in))
      out <- Resource.eval(read(dataClient, tbl))
    } yield (out.size must be_===(nr * 3))
  }
}
