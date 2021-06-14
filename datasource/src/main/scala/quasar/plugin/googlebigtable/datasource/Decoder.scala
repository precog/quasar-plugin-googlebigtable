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

import qdata.{QDataDecode, QType}
import qdata.time.{DateTimeInterval, OffsetDate}

import quasar.contrib.std.errorImpossible

import java.time.OffsetDateTime
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetTime
import java.time.LocalDate

import com.google.cloud.bigtable.data.v2.models.Row
import spire.math.Real

object Decoder {
  val qdataDecode: QDataDecode[Row] = new QDataDecode[Row] {

    override def tpe(a: Row): QType = errorImpossible

    override def getLong(a: Row): Long = errorImpossible

    override def getDouble(a: Row): Double = errorImpossible

    override def getReal(a: Row): Real = errorImpossible

    override def getString(a: Row): String = errorImpossible

    override def getBoolean(a: Row): Boolean = errorImpossible

    override def getLocalDateTime(a: Row): LocalDateTime = errorImpossible

    override def getLocalDate(a: Row): LocalDate = errorImpossible

    override def getLocalTime(a: Row): LocalTime = errorImpossible

    override def getOffsetDateTime(a: Row): OffsetDateTime = errorImpossible

    override def getOffsetDate(a: Row): OffsetDate = errorImpossible

    override def getOffsetTime(a: Row): OffsetTime = errorImpossible

    override def getInterval(a: Row): DateTimeInterval = errorImpossible

    override def getArrayCursor(a: Row): ArrayCursor = errorImpossible

    override def hasNextArray(ac: ArrayCursor): Boolean = errorImpossible

    override def getArrayAt(ac: ArrayCursor): Row = errorImpossible

    override def stepArray(ac: ArrayCursor): ArrayCursor = errorImpossible

    override def getObjectCursor(a: Row): ObjectCursor = errorImpossible

    override def hasNextObject(ac: ObjectCursor): Boolean = errorImpossible

    override def getObjectKeyAt(ac: ObjectCursor): String = errorImpossible

    override def getObjectValueAt(ac: ObjectCursor): Row = errorImpossible

    override def stepObject(ac: ObjectCursor): ObjectCursor = errorImpossible

    override def getMetaValue(a: Row): Row = errorImpossible

    override def getMetaMeta(a: Row): Row = errorImpossible

  }
}
