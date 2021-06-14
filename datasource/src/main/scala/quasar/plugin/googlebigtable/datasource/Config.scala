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

import quasar.api.resource.{ResourceName, ResourcePath}

import cats.effect.Sync
import com.google.auth.oauth2.GoogleCredentials
import com.precog.googleauth.{Credentials, ServiceAccount}
import pathy.Path

final case class TableName(value: String)

final case class InstanceId(value: String)

final case class RowPrefix(value: String)

final case class Config(serviceAccount: ServiceAccount, instanceId: InstanceId, tableName: TableName, rowPrefix: RowPrefix) {

  val Scope = "https://www.googleapis.com/auth/cloud-platform"

  def sanitize: Config = this

  def instancePath: String = s"projects/${serviceAccount.projectId}/instances/$instanceId"

  def credentials[F[_]: Sync]: F[GoogleCredentials] =
    Credentials.googleCredentials(serviceAccount.serviceAccountAuthBytes, Scope)

  val resourceName: ResourceName = ResourceName(tableName.value + rowPrefix.value)

  val resourcePath: ResourcePath =
    ResourcePath.leaf(Path.rootDir </> Path.file(resourceName.value))
}