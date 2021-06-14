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
import cats.effect.testing.specs2.CatsIO

import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.models.{Query, Row}

import org.specs2.mutable.Specification

/**
  * Initializes a test instance, given that it is created and running.
  */
class InitializeSpec extends Specification with CatsIO {
  import BigTableSpecUtils._



  def read(dataClient: BigtableDataClient, tableId: String): IO[List[Row]] =
    IO.delay {
      val query = Query.create(tableId)
      dataClient.readRows(query).iterator.asScala.toList
    }

  "initialize" >> {
    for {
      config <- Resource.liftF(testConfig[IO])
      adminClient <- GoogleBigTable.adminClient[IO](config)
      dataClient <- GoogleBigTable.dataClient[IO](config)
      (_, t) <- table(adminClient)
      _ <- Resource.liftF(writeToTable(dataClient, t, ColumnFamily))
      rows <- Resource.liftF(read(dataClient, t))
    } yield (rows.size must be_===(2))
  }
}
