package strd.common

/**
 *
 * Created by penkov on 24.11.14.
 */
case class AuthId(profileId: Int, ip: String)

class Request(authId: AuthId) extends Serializable{
  val auth : AuthId = authId
}
