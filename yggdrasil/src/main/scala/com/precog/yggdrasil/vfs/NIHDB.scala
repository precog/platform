package com.precog
package yggdrasil
package vfs

case class NIHDBResource(db: NIHDB)(implicit as: ActorSystem) extends Resource {
  def mimeType: Future[MimeType] = Future(Resource.QuirrelData)
  def authorities: Future[Authorities] = db.authorities
  def close: Future[PrecogUnit] = db.close(as)
  def append(data: PathData): Future[PrecogUnit] = data match {
    case NIHDBData(batch) =>
      db.insert(batch)

    case _ => Promise.failed(new IllegalArgumentException("Attempt to insert non-event data to NIHDB"))
  }
}
