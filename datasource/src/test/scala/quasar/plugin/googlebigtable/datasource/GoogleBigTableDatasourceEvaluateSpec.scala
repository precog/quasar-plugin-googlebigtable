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
import quasar.api.DataPathSegment
import quasar.api.push.InternalKey
import quasar.api.resource.ResourcePath
import quasar.common.data.RValue
import quasar.connector.{Offset, QueryResult}
import quasar.qscript.InterpretedRead

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.implicits._

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.data.v2.BigtableDataClient

import fs2.Stream

import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification

import skolems.∃
import spire.math.Real

class GoogleBigTableDatasourceEvaluateSpec extends Specification with DsIO {

  import DsIO._

  skipAllIf(!runITs)

  private def harnessed(rowPrefix: RowPrefix, columnFamilies: List[String])
      : Resource[IO, (GoogleBigTableDatasource[IO], BigtableTableAdminClient, BigtableDataClient, ResourcePath, TableName)] =
    tableHarness(rowPrefix, columnFamilies)

  private def loadRows(ds: GoogleBigTableDatasource[IO], p: ResourcePath): IO[List[RValue]] =
    ds.loadFull(InterpretedRead(p, ScalarStages.Id)).value use {
      case Some(QueryResult.Parsed(_, res, _)) =>
        res.data.asInstanceOf[Stream[IO, RValue]].compile.to(List)

      case _ => IO.pure(List[RValue]())
    }

  private def seekRows(ds: GoogleBigTableDatasource[IO], p: ResourcePath, offset: Offset): IO[List[RValue]] =
    ds.loadFrom(InterpretedRead(p, ScalarStages.Id), offset).value use {
      case Some(QueryResult.Parsed(_, res, _)) =>
        res.data.asInstanceOf[Stream[IO, RValue]].compile.to(List)

      case _ => IO.pure(List[RValue]())
    }

  private def testTemplate(rowPrefix: RowPrefix, columnFamilies: List[String], rowsSetup: List[TestRow], expected: List[TestRow]): IO[MatchResult[List[RValue]]] = {
    harnessed(rowPrefix, columnFamilies) use { case (ds, adminClient, dataClient, path, tableName) =>
      val setup = writeToTable(dataClient, rowsSetup.map(_.toRowMutation(tableName)))

      (setup >> loadRows(ds, path)) map { results =>
        val exp = expected.map(tr => Evaluator.toRValue(tr.toRow))
        results must containTheSameElementsAs(exp)
      }
    }
  }

  private def testTemplateSeek(offset: Offset, rowPrefix: RowPrefix, columnFamilies: List[String], rowsSetup: List[TestRow], expected: List[TestRow]): IO[MatchResult[List[RValue]]] = {
    harnessed(rowPrefix, columnFamilies) use { case (ds, adminClient, dataClient, path, tableName) =>
      val setup = writeToTable(dataClient, rowsSetup.map(_.toRowMutation(tableName)))

      (setup >> seekRows(ds, path, offset)) map { results =>
        val exp = expected.map(tr => Evaluator.toRValue(tr.toRow))
        results must containTheSameElementsAs(exp)
      }
    }
  }

  "loading data" >> {
    "string" >> {
      val cf1 = "cf1"
      val cf2 = "cf2"
      val row1 = TestRow("rowKey1", List(mkRowCell(cf1, "greeting", 1L, "Hello World")))
      val row2 = TestRow("rowKey2", List(mkRowCell(cf1, "greeting", 3L, "Hey Joe!"), mkRowCell(cf1, "name", 2L, "Joe")))
      val row3 = TestRow("rowKey3", List(mkRowCell(cf1, "greeting", 5L, "Bon Jour!"), mkRowCell(cf2, "name", 4L, "Jour")))

      "single row, single cell" >> {
        val rows = List(row1)
        testTemplate(RowPrefix(""), List(cf1), rows, rows)
      }

      "single row, multiple cells in same family" >> {
        val rows = List(row2)
        testTemplate(RowPrefix(""), List(cf1), rows, rows)
      }

      "single row, multiple cells in different families" >> {
        val rows = List(row3)
        testTemplate(RowPrefix(""), List(cf1, cf2), rows, rows)
      }

      "multiple rows" >> {
        val rows = List(row1, row2, row3)
        testTemplate(RowPrefix(""), List(cf1, cf2), rows, rows)
      }

      "single row, matching prefix" >> {
        val rows = List(row3)
        testTemplate(RowPrefix("rowKey"), List(cf1, cf2), rows, rows)
      }

      "single row, non-matching prefix" >> {
        val rows = List(row3)
        testTemplate(RowPrefix("nope"), List(cf1, cf2), rows, List.empty)
      }

      "multiple rows, partly matching prefix" >> {
        val rows = List(row1, row2, row3).flatMap(r => List(r, r.copy(key = "nope" + r.key), r.copy(key = r.key + "suffix")))
        val expected = List(row1, row2, row3).flatMap(r => List(r, r.copy(key = r.key + "suffix")))
        testTemplate(RowPrefix("rowKey"), List(cf1, cf2), rows, expected)
      }
    }
  }

  "seeking data" >> {
    "string rowKey" >> {
      val cf1 = "cf1"
      val before = TestRow("before", List(mkRowCell(cf1, "a", 1L, "oops")))
      val after = TestRow("zafter", List(mkRowCell(cf1, "a", 1L, "nope")))
      val row1 = TestRow("rowKey1", List(mkRowCell(cf1, "a", 1L, "foo")))
      val row2 = TestRow("rowKey2", List(mkRowCell(cf1, "a", 3L, "bar")))
      val row3 = TestRow("rowKey3", List(mkRowCell(cf1, "a", 5L, "baz")))

      def offset(s: String) =
        Offset.Internal(NonEmptyList.one(DataPathSegment.Field("key")), ∃(InternalKey.Actual.string(s)))

      "with prefix" >> {
        testTemplateSeek(offset("rowKey2"), RowPrefix("rowKey"), List(cf1), List(before, after, row1, row2, row3), List(row2, row3))
      }

      "without prefix" >> {
        testTemplateSeek(offset("rowKey2"), RowPrefix(""), List(cf1), List(before, after, row1, row2, row3), List(row2, row3, after))
      }
    }

    "number timestamp" >> {
      val cf1 = "cf1"
      val cf2 = "cf2"
      val before = TestRow("before", List(mkRowCell(cf1, "a", 1L, "oops a"), mkRowCell(cf2, "b", 2L, "oops b")))
      val after = TestRow("zafter", List(mkRowCell(cf1, "a", 1L, "nope a"), mkRowCell(cf2, "b", 10L, "nope b")))
      val row1 = TestRow("rowKey1", List(mkRowCell(cf1, "a", 1L, "foo a"), mkRowCell(cf2, "b", 4L, "foo b")))
      val row2 = TestRow("rowKey2", List(mkRowCell(cf1, "a", 2L, "bar"), mkRowCell(cf2, "b", 3L, "bar")))
      val row3 = TestRow("rowKey3", List(mkRowCell(cf1, "a", 2L, "baz"), mkRowCell(cf2, "b", 5L, "baz")))

      def offset(r: Real) =
        Offset.Internal(NonEmptyList.one(DataPathSegment.Field("timestamp")), ∃(InternalKey.Actual.real(r)))

      "with prefix" >> {
        testTemplateSeek(offset(Real(4)), RowPrefix("rowKey"), List(cf1, cf2), List(before, after, row1, row2, row3), List(row1, row3))
      }

      "without prefix" >> {
        testTemplateSeek(offset(Real(4)), RowPrefix(""), List(cf1, cf2), List(before, after, row1, row2, row3), List(row1, row3, after))
      }
    }
  }
}
