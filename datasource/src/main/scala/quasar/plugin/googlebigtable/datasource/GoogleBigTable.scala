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

import cats.effect.{Resource, Sync}
import cats.implicits._

import com.google.api.gax.core.FixedCredentialsProvider
import com.google.cloud.bigtable.admin.v2.{BigtableTableAdminClient, BigtableTableAdminSettings}
import com.google.cloud.bigtable.data.v2.{BigtableDataClient, BigtableDataSettings}

object GoogleBigTable {
  def dataSettings[F[_]: Sync](config: Config): F[BigtableDataSettings] =
    for {
      creds <- config.credentials
      settings <-
        Sync[F].delay {
          val credsProv = FixedCredentialsProvider.create(creds)
          BigtableDataSettings
            .newBuilder()
            .setProjectId(config.serviceAccount.projectId)
            .setInstanceId(config.instanceId.value)
            .setCredentialsProvider(credsProv)
            .build()
        }
    } yield settings

  def dataClient[F[_]: Sync](config: Config): Resource[F, BigtableDataClient] = {
    val acq =
      dataSettings(config)
        .flatMap(s => Sync[F].delay(BigtableDataClient.create(s)))
    Resource.fromAutoCloseable(acq)
  }

  def adminSettings[F[_]: Sync](config: Config): F[BigtableTableAdminSettings] =
    for {
      creds <- config.credentials
      settings <-
        Sync[F].delay {
          val credsProv = FixedCredentialsProvider.create(creds)
          BigtableTableAdminSettings
            .newBuilder()
            .setProjectId(config.serviceAccount.projectId)
            .setInstanceId(config.instanceId.value)
            .setCredentialsProvider(credsProv)
            .build()
        }
    } yield settings

  def adminClient[F[_]: Sync](config: Config): Resource[F, BigtableTableAdminClient] = {
    val acq =
      adminSettings(config)
        .flatMap(s => Sync[F].delay(BigtableTableAdminClient.create(s)))
    Resource.fromAutoCloseable(acq)
  }
}
