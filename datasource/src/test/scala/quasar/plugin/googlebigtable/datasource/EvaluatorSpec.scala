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

    def mkValTsRObject(value: String, ts: Long) =
      RObject(
        "value" -> CString(value),
        "timestamp" -> CLong(ts))

    val row = TestRow("rowKey1", List(
      mkRowCell("cf1", "a", 1L, "foo"),
      mkRowCell("cf1", "b", 2L, "bar"),
      mkRowCell("cf2", "c", 3L, "baz"),
      mkRowCell("cf2", "d", 4L, "ok"),
      mkRowCell("cf2", "e", 5L, "yo")))

    "simple" >> {
      Evaluator.toRValue(row.toRow) must_== RObject(Map(
        "key" -> CString("rowKey1"),
        "cells" -> RObject(Map(
          "cf1" -> RObject(
            "a" -> mkValTsRObject("foo", 1000L),
            "b" -> mkValTsRObject("bar", 2000L)),
          "cf2" -> RObject(
            "c" -> mkValTsRObject("baz", 3000L),
            "d" -> mkValTsRObject("ok", 4000L),
            "e" -> mkValTsRObject("yo", 5000L))))))
    }
  }
}
