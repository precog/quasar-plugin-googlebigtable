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

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Paths
import java.nio.file.Files
import argonaut._
import cats.implicits._
import cats.effect.Sync

object  BigTableSpecUtils {
  val AuthResourceName = "precog-ci-275718-6d5ee6b82f02.json"
  val InstanceName = "precog-test"
  val TableId = "test-table"
  val ColumnFamily = "cf1"

  def readServiceAccount[F[_]: Sync](authResourceName: String): F[ServiceAccount] =
    for {
      authCfgPath <- Sync[F].delay(Paths.get(getClass.getClassLoader.getResource(authResourceName).toURI))
      authCfgString <- Sync[F].delay(new String(Files.readAllBytes(authCfgPath), UTF_8))
      sa <-
        Parse.parse(authCfgString) match {
          case Left(_) => Sync[F].raiseError[ServiceAccount](new RuntimeException("Malformed auth config"))
          case Right(json) => json.as[ServiceAccount].fold(
            (_, _) => Sync[F].raiseError[ServiceAccount](new RuntimeException("Json is not valid ServiceAccount")),
            _.pure[F])
        }
    } yield sa

  def testConfig[F[_]: Sync]: F[Config] = readServiceAccount[F](AuthResourceName).map(Config(InstanceName, _))
}
