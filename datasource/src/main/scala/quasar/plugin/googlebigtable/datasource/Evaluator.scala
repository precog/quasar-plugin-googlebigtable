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

import java.lang.Math
import scala.collection.JavaConverters._

import cats.implicits._
import cats.effect.{ConcurrentEffect, Sync}

import com.google.api.gax.rpc.StreamController
import com.google.api.gax.rpc.StateCheckingResponseObserver
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.models.{Query => GQuery, Row}

import fs2.Stream

class Evaluator[F[_]: ConcurrentEffect](client: BigtableDataClient, query: GQuery, maxQueueSize: Int) {
  import Evaluator._

  def evaluate(): Stream[F, RValue] = {
    val handler = Observer.handler[F](client.readRowsAsync(query, _))
    CallbackHandler.toStream[F, Row](handler, maxQueueSize).map(toRValue(_))
  }
}

object Evaluator {

  val DefaultMaxQueueSize = 10

  def apply[F[_]: ConcurrentEffect](client: BigtableDataClient, query: GQuery, maxQueueSize: Int): Evaluator[F] =
    new Evaluator[F](client, query, maxQueueSize)

  def toRValue(row: Row): RValue = {
    val (ts: Long, values: Map[String, Map[String, (Long, RValue)]]) =
      row.getCells.asScala.toList.foldLeft((0L, Map.empty[String, Map[String, (Long, RValue)]])) { case ((ts, m), cell) =>
        val entry = (cell.getQualifier.toStringUtf8, (cell.getTimestamp(), CString(cell.getValue.toStringUtf8)))
        val fetchedValue = m.getOrElse(cell.getFamily(), Map.empty[String, (Long, RValue)]).get(cell.getQualifier.toStringUtf8)
        val (tsw, entryToWrite) = toWrite(entry, fetchedValue)
        val maxTs = Math.max(ts, tsw)
        val newMap = entryToWrite.fold(m)(e => m + ((cell.getFamily(), m.getOrElse(cell.getFamily(), Map.empty[String, (Long, RValue)]) + e)))
        (maxTs / 1000, newMap)
      }
    RObject(
      "key" -> CString(row.getKey().toStringUtf8()),
      "timestamp" -> CLong(ts),
      "cells" -> RObject(values.mapValues(x => RObject(x.mapValues(_._2)))))
  }

  private def toWrite(entry: (String, (Long, RValue)), fetched: Option[(Long, RValue)]): (Long, Option[(String, (Long, RValue))]) =
    fetched.fold((entry._2._1, entry.some)) { f =>
      if (f._1 >= entry._2._1) (f._1, none)
      else (entry._2._1, entry.some)
    }

  class Observer[F[_]: Sync](callback: CallbackHandler.Callback[F, Row]) extends StateCheckingResponseObserver[Row] {

    override protected def onStartImpl(controller: StreamController): Unit =
      ()

    override protected def onResponseImpl(row: Row): Unit =
      callback(Right(Some(Sync[F].delay(row))))

    override protected def onErrorImpl(t: Throwable): Unit =
      callback(Left(t))

    override protected def onCompleteImpl(): Unit =
      callback(Right(None))

  }

  object Observer {
    def handler[F[_]: Sync](f: Observer[F] => Unit): (Either[Throwable, Option[F[Row]]] => Unit) => F[Unit] = { cb =>
      Sync[F].delay {
        f(new Observer(cb))
      }
    }
  }
}
