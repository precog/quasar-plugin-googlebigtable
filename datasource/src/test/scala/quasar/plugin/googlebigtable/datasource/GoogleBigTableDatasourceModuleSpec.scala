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

import quasar.RateLimiter
import quasar.api.datasource.DatasourceError, DatasourceError._
import quasar.connector.ByteStore
import quasar.connector.datasource.{LightweightDatasourceModule => DSM}

import java.util.UUID

import argonaut._, Argonaut._

import cats.effect.{Resource, IO}
import cats.implicits._

import org.specs2.mutable.Specification

import json._

class GoogleBigTableDatasourceModuleSpec extends Specification with DsIO {

  skipAllIf(!runITs)

  def mkDs(j: Json)
      : Resource[IO, Either[InitializationError[Json], DSM.DS[IO]]] =
    RateLimiter[IO, UUID](IO(UUID.randomUUID()))
      .flatMap(rl =>
        GoogleBigTableDatasourceModule.lightweightDatasource[IO, UUID](j, rl, ByteStore.void[IO], _ => IO(None)))

  "datasource init" >> {
    "succeeds when correct cfg" >> {
      Resource.eval(testConfig[IO](TableName("x"), RowPrefix("y")))
        .flatMap(cfg => mkDs(cfg.asJson))
        .map(_ must beRight)
    }

    "fails with invalid config when invalid json" >> {
      mkDs("invalid".asJson).map(_ must beLike {
        case Left(InvalidConfiguration(_, _, _)) => ok
      })
    }
  }
}
