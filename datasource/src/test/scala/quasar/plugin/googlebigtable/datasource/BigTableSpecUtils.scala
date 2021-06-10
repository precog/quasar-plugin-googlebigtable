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

import cats.implicits._
import cats.effect.Sync
import com.precog.googleauth.ServiceAccount

object  BigTableSpecUtils {
  val AuthResourceName = "precog-ci-275718-6d5ee6b82f02.json"
  val InstanceName = "precog-test"
  val TableId = "test-table"
  val ColumnFamily = "cf1"

  def testConfig[F[_]: Sync]: F[Config] =
    ServiceAccount.fromResourceName[F](AuthResourceName).map(Config(InstanceName, _))
}
