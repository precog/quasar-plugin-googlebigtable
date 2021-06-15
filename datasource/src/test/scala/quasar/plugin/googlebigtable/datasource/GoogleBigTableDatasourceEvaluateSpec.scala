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

import quasar.ScalarStages
import quasar.api.resource.ResourcePath
import quasar.connector.{QueryResult, ResourceError}
import quasar.contrib.scalaz.MonadError_
import quasar.qscript.InterpretedRead

import cats.effect.{IO, Resource}
import cats.effect.testing.specs2.CatsIO
import cats.implicits._

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.models.Row

import fs2.Stream

import org.specs2.mutable.Specification

class GoogleBigTableDatasourceEvaluateSpec extends Specification with CatsIO {

  import BigTableSpecUtils._

  private def harnessed(rowPrefix: RowPrefix, columnFamilies: List[String])
      : Resource[IO, (GoogleBigTableDatasource[IO], BigtableTableAdminClient, BigtableDataClient, ResourcePath, TableName)] =
    tableHarness(rowPrefix, columnFamilies)

  private def loadRows(ds: GoogleBigTableDatasource[IO], p: ResourcePath): IO[List[Row]] =
    ds.loadFull(InterpretedRead(p, ScalarStages.Id)).value use {
      case Some(QueryResult.Parsed(_, res, _)) =>
        res.data.asInstanceOf[Stream[IO, Row]].compile.to(List)

      case _ => IO.pure(List[Row]())
    }

  "loading data" >> {
    "string" >> {
      val cf = "cf1"
      harnessed(RowPrefix(""), List(cf)) use { case (ds, adminClient, dataClient, path, tableName) =>
        val rows = List(TestRow("rowKey1", List(mkRowCell(cf, "greeting", 1L, "Hey Joe!"))))

        val setup = writeToTable(dataClient, rows.map(_.toRowMutation(tableName)))

        (setup >> loadRows(ds, path)) map { results =>
          val expected = rows.map(_.toRow)
          results must containTheSameElementsAs(expected)
        }
      }
    }
  }
}

object GoogleBigTableDatasourceEvaluateSpec {
  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)
}
