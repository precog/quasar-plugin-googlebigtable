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

import quasar.common.data.{CLong, CString, RObject, RValue}

import scala.collection.JavaConverters._

import cats.effect.ConcurrentEffect

import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.models.{Row, RowCell}

import fs2.Stream

class Evaluator[F[_]: ConcurrentEffect](client: BigtableDataClient, query: Query, maxQueueSize: Int) {
  import Evaluator._

  def evaluate(): Stream[F, RValue] = {
    val handler = Observer.handler[F](client.readRowsAsync(query.googleQuery, _))
    CallbackHandler.toStream[F, Row](handler, maxQueueSize).map(toRValue(_))
  }

}

object Evaluator {

  val DefaultMaxQueueSize = 10

  def apply[F[_]: ConcurrentEffect](client: BigtableDataClient, query: Query, maxQueueSize: Int): Evaluator[F] =
    new Evaluator[F](client, query, maxQueueSize)

  def toRValue(row: Row): RValue = {
    val values: Map[String, Map[String, RValue]] = row.getCells.asScala.toList.foldLeft(Map.empty[String, Map[String, RValue]]) { case (m, cell) =>
      m + ((cell.getFamily(), m.getOrElse(cell.getFamily(), Map.empty[String, RValue]) + rowCellToRObjectEntry(cell)))
    }
    RObject(row.getKey().toStringUtf8() -> RObject(values.mapValues(RObject(_))))
  }

  private def rowCellToRObjectEntry(rowCell: RowCell): (String, RValue) = {
    val rv = RObject(
      "value" -> CString(rowCell.getValue.toStringUtf8),
      // TODO support labels?
      //"labels" -> RArray(rowCell.getLabels().asScala.map(CString(_)).toList),
      "timestamp" -> CLong(rowCell.getTimestamp()))
    (rowCell.getQualifier.toStringUtf8, rv)
  }
}
