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

import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}
import quasar.connector.datasource.{DatasourceSpec, LightweightDatasourceModule}

import cats.effect.{IO, Resource}
import fs2.Stream

class GoogleBigTableDatasourceSpec extends DatasourceSpec[IO, Stream[IO, ?], ResourcePathType.Physical] {

  import BigTableSpecUtils._

  def mkDatasource(config: Config): Resource[IO, LightweightDatasourceModule.DS[IO]] =
    GoogleBigTableDatasource[IO](config)

  val nonExistentPath =
    ResourcePath.root() / ResourceName("does") / ResourceName("not") / ResourceName("exist")

  val datasource = Resource.liftF(testConfig[IO]).flatMap(mkDatasource(_))

  def gatherMultiple[A](g: Stream[IO, A]) = g.compile.toList

  "an actual table is a resource" >>* {
    val res = ResourcePath.root() / ResourceName("test-table")
    datasource.flatMap(_.pathIsResource(res)).use(b => IO.pure(b must beTrue))
  }

}
