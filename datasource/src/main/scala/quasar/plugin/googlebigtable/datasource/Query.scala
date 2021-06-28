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

import quasar.api.DataPathSegment
import quasar.api.push.{InternalKey, OffsetPath}

import scala.Predef._

import cats.Id
import cats.data.NonEmptyList
import cats.implicits._
import com.google.cloud.bigtable.data.v2.{models => g}, g.Filters.FILTERS, g.Range.ByteStringRange
import monocle.Prism
import skolems.∃

final case class Query(tableName: TableName, rowPrefix: RowPrefix, offset: Option[(Query.OffsetField, ∃[InternalKey.Actual])]) {
  lazy val googleQuery: Either[String, g.Query] =
    offset match {
      case Some((field, key)) =>
        field match {
          case Query.Timestamp =>
            extractRangeStartLong(key) match {
              case Some(l) =>
                val pred = FILTERS.timestamp().range().startClosed(l * 1000).endUnbounded()
                val filter = FILTERS.condition(pred).`then`(FILTERS.pass()).otherwise(FILTERS.block())
                g.Query
                  .create(tableName.value)
                  .filter(filter)
                  .range(getPrefixRange)
                  .asRight
              case None => "Unexpected type for timestamp offset".asLeft
            }
          case Query.Key =>
            extractRangeStartString(key) match {
              case Some(s) =>
                g.Query
                  .create(tableName.value)
                  .range(getPrefixRange.startClosed(s))
                  .asRight
              case None => "Unexpected type for key offset".asLeft
            }
        }
      case None =>
        g.Query
          .create(tableName.value)
          .range(getPrefixRange)
          .asRight
    }

  private def getPrefixRange: ByteStringRange =
    ByteStringRange.prefix(rowPrefix.value)

  private def extractRangeStartString(key: ∃[InternalKey.Actual]): Option[String] = {
    val actual: InternalKey[Id, _] = key.value

    actual match {
      case InternalKey.StringKey(s) => Some(s)
      case _ => None
    }
  }

  private def extractRangeStartLong(key: ∃[InternalKey.Actual]): Option[Long] = {
    val actual: InternalKey[Id, _] = key.value

    actual match {
      case InternalKey.RealKey(r) => Try(r.toLong).toOption
      case _ => None
    }
  }
}

object Query {
  trait OffsetField {
    val name: String
    def path(): OffsetPath =
      NonEmptyList(DataPathSegment.Field(name), List())
  }
  case object Key extends OffsetField {
    val name = "key"
  }
  case object Timestamp extends OffsetField {
    val name = "timestamp"
  }

  private def fromName(name: String) = name match {
    case Key.name => Key.some
    case Timestamp.name => Timestamp.some
  }

  val offsetFieldPrism: Prism[OffsetPath, OffsetField] =
    Prism[OffsetPath, OffsetField] {
      case NonEmptyList(DataPathSegment.Field(name), List()) => fromName(name)
      case _ => None
    } (_.path())

}
