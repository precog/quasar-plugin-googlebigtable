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

import quasar.api.resource.{/:, ResourcePath}

import cats.effect.Sync
import com.google.auth.oauth2.GoogleCredentials
import com.precog.googleauth.{Credentials, ServiceAccount}
import monocle.Prism

final case class TableName(value: String)

object TableName {
  def fromResourcePath(path: ResourcePath): Option[TableName] = path match {
    case ResourcePath.Root => None
    case t /: ResourcePath.Root => Some(TableName(t))
    case _ => None
  }
}

final case class InstanceId(value: String)

final case class RowPrefix(value: String)

final case class Config(serviceAccount: ServiceAccount, instanceId: InstanceId, tableName: TableName, rowPrefix: RowPrefix) {

  val Scope = "https://www.googleapis.com/auth/cloud-platform"

  def sanitize: Config = this

  def instancePath: String = s"projects/${serviceAccount.projectId}/instances/$instanceId"

  def credentials[F[_]: Sync]: F[GoogleCredentials] =
    Credentials.googleCredentials(serviceAccount.serviceAccountAuthBytes, Scope)

  private def tablesPath = instancePath + "/tables/"

  def tableNamePrism: Prism[String, TableName] =
    Prism[String, TableName](extractTableName)(t => tablesPath + t.value)

  private def extractTableName(fullName: String): Option[TableName] =
    if (fullName.startsWith(tablesPath))
      Some(TableName(fullName.substring(tablesPath.length)))
    else
      None
}
