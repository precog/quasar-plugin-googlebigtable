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

import quasar.api.push.InternalKey

import cats.Id
import com.google.cloud.bigtable.data.v2.{models => g}, g.Range.ByteStringRange
import skolems.∃

final case class Query(tableName: TableName, rowPrefix: RowPrefix, offset: Option[∃[InternalKey.Actual]]) {
  lazy val googleQuery: g.Query =
     g.Query
        .create(tableName.value)
        .range(getRange)

  private def getRange: ByteStringRange = {
    val r = ByteStringRange.prefix(rowPrefix.value)
    val rangeStart = offset.flatMap(off => extractRangeStart(off))
    rangeStart.fold(r)(rs => r.startClosed(rs))
  }

  private def extractRangeStart(key: ∃[InternalKey.Actual]): Option[String] = {
    val actual: InternalKey[Id, _] = key.value

    actual match {
      case InternalKey.StringKey(s) => Some(s)
      case _ => None
    }
  }

}
