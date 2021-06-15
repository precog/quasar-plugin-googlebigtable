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

import cats.effect.{ConcurrentEffect, Resource}
import cats.implicits._

import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.{models => g}

import fs2.Stream

class Evaluator[F[_]: ConcurrentEffect](client: BigtableDataClient, tableName: TableName, offset: Any, stages: ScalarStages) {

  def evaluate(): Resource[F, (ScalarStages, Stream[F, g.Row])] = {
    val s = execQuery(Query(tableName, RowPrefix("")))
    Resource.liftF((stages, s).pure[F])
  }

  def execQuery(query: Query): Stream[F, g.Row] = {
    val gquery = g.Query.create(query.tableName.value)
    val handler = Observer.handler[F](client.readRowsAsync(gquery, _))
    CallbackHandler.toStream[F, g.Row](handler, 50)
  }

}

object Evaluator {

  def apply[F[_]: ConcurrentEffect](client: BigtableDataClient, tableName: TableName, offset: Any, stages: ScalarStages): Evaluator[F] =
    new Evaluator[F](client, tableName, offset, stages)

}
