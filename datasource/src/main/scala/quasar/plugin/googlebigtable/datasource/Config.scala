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

import java.io.ByteArrayInputStream

import cats.effect.Sync
import com.google.auth.oauth2.GoogleCredentials
import monocle.Prism

final case class TableName(value: String)

final case class Config(instanceId: String, serviceAccount: ServiceAccount) {
  
  def sanitize: Config = this

  def instancePath: String = s"projects/${serviceAccount.projectId}/instances/$instanceId"

  def credentials[F[_]: Sync]: F[GoogleCredentials] = Sync[F].delay(
    GoogleCredentials
      .fromStream(new ByteArrayInputStream(serviceAccount.serviceAccountAuthBytes))
      .createScoped("https://www.googleapis.com/auth/cloud-platform"))

  private def tablesPath = instancePath + "/tables/"

  def tableNamePrism: Prism[String, TableName] =
    Prism[String, TableName](extractTableName)(t => tablesPath + t.value)

  private def extractTableName(fullName: String): Option[TableName] =
    if (fullName.startsWith(tablesPath)) 
      Some(TableName(fullName.substring(tablesPath.length)))
    else
      None
}