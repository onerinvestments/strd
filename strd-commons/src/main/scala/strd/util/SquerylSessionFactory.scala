package strd.util

import org.squeryl.{Session, SessionFactory}
import org.squeryl.adapters.PostgreSqlAdapter

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 9/30/13
 * Time: 3:40 PM
 */

trait SquerylSessionFactory{ this : ClusterStrdConf =>


  def configureSqueryl() {
    val ds = conf.pooledDS(1)

    SessionFactory.concreteFactory = Some(()=>
     Session.create(
          ds.getConnection,
          new PostgreSqlAdapter))

  }
  println("--> SquerylSessionFactory")
  configureSqueryl()

}
