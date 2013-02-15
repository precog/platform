package com.precog.yggdrasil
package table

import TableModule._

trait NIHDBColumnarTableModule[M[+_], Long] extends BlockStoreColumnarTableModule[M] with ProjectionModule[M, Key, Slice] with StorageMetadataSource[M] {
  def accessControl: Accesscontrol[M]

  def load(table: Table, apiKey: APIKey, tpe: JType): M[Table] = {
    import loadMergeEngine._

    val constraints = Schema.flatten(tpe)

    for {
      paths          <- pathsM(table)
      projections    <- paths.map { path =>
        for {
          proj <- Projection(path)
          canAccess <- accessControl.hasCapability(apiKey, Set(ReducePermission(path, proj.ownerAccountIds)), some(new DateTime))
        } yield {
          if (canAccess) Some(proj) else {
            close(proj)
            None
          }
        }
      }.sequence map (_.flatten)
    } yield {
      def slices(proj: Projection): StreamT[M, Slice] = {
        StreamT.unfoldM[M, Slice, Option[Long]](None) { key =>
          proj.getBlockAfter(key, constraints).map(_.map { case BlockProjectionData(_, maxKey, slice) => (slice, Some(maxKey)) })
        }
      }

      Table(projections.foldLeft(StreamT.empty[M, Slice]) { (acc, proj) => acc ++ slices(proj) })
    }
  }


}
