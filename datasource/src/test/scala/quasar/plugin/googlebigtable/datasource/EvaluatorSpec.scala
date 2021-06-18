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

import quasar.common.data.{CLong, CString, RObject}

import org.specs2.mutable.Specification

object EvaluatorSpec extends Specification with DsIO {

  import DsIO._

  "toRValue" >> {

    def mkValTsLabelsRObject(value: String, ts: Long, ls: List[String] = List.empty) =
      RObject(
        "value" -> CString(value),
        // TODO support labels?
        // "labels" -> RArray(ls.map(CString(_))),
        "timestamp" -> CLong(ts))

    val row = TestRow("rowKey1", List(
      mkRowCell("cf1", "a", 1L, "foo", List("l1", "l2")),
      mkRowCell("cf1", "b", 2L, "bar"),
      mkRowCell("cf2", "c", 3L, "baz", List("l3")),
      mkRowCell("cf2", "d", 4L, "ok", List("l1")),
      mkRowCell("cf2", "e", 5L, "yo", List("l2"))))

    "simple" >> {
      Evaluator.toRValue(row.toRow) must_== RObject(Map(
        "key" -> CString("rowKey1"),
        "cells" -> RObject(Map(
          "cf1" -> RObject(
            "a" -> mkValTsLabelsRObject("foo", 1000L, List("l1", "l2")),
            "b" -> mkValTsLabelsRObject("bar", 2000L)),
          "cf2" -> RObject(
            "c" -> mkValTsLabelsRObject("baz", 3000L, List("l3")),
            "d" -> mkValTsLabelsRObject("ok", 4000L, List("l1")),
            "e" -> mkValTsLabelsRObject("yo", 5000L, List("l2")))))))
    }
  }
}
