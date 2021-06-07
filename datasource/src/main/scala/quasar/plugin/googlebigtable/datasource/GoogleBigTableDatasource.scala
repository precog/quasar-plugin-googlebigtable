package quasar.plugin.googlebigtable.datasource

import slamdata.Predef._

import quasar.api.datasource.DatasourceType
import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}
import quasar.connector._
import quasar.connector.datasource.{BatchLoader, LightweightDatasource, Loader}
import quasar.qscript.InterpretedRead

import cats.data.NonEmptyList
import cats.effect._
import fs2.Stream

final class GoogleBigTableDatasource[F[_]: Sync: MonadResourceErr](
    config: Config)
    extends LightweightDatasource[Resource[F, ?], Stream[F, ?], QueryResult[F]] {

    def kind: DatasourceType = GoogleBigTableDatasourceModule.kind

    def loaders: NonEmptyList[Loader[Resource[F,*], InterpretedRead[ResourcePath], QueryResult[F]]] = 
      NonEmptyList.of(Loader.Batch(BatchLoader.Full { (iRead: InterpretedRead[ResourcePath]) =>
        iRead.path match {
        case ResourcePath.Root => ???
        case ResourcePath.Leaf(file) => ???
      }
    }))

    def pathIsResource(path: ResourcePath): Resource[F, Boolean] = ???

    def prefixedChildPaths(prefixPath: ResourcePath)
        : Resource[F, Option[Stream[F, (ResourceName, ResourcePathType.Physical)]]] = ???

}