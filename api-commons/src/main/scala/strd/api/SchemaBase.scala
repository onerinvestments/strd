package strd.api

import java.sql.Timestamp

import org.squeryl.annotations._
import org.squeryl.{KeyedEntity, PrimitiveTypeMode}
import utl.rtb.model.AdvertisingStatus


/**
 *
 * User: light
 * Date: 18/10/14
 * Time: 18:07
 */

object SchemaBase extends PrimitiveTypeMode {

}

trait Timestampable {
  val created = new Timestamp(System.currentTimeMillis())
  var modified = created

  def modify() = {
    modified = new Timestamp(System.currentTimeMillis())
  }
}

trait Entity extends KeyedEntity[Int] with Timestampable {
  val id: Int = 0
}



trait LifeCycle {
  @Column("status")
  var _status = AdvertisingStatus.INACTIVE.id
  def status = AdvertisingStatus.forId(_status)
  def status_=(s: AdvertisingStatus) {// Add workflow checks when new statuses will be introduced
    _status = s.id
  }
}

