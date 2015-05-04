package strd.dht

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 17/02/14
 * Time: 15:33
 */
object DHTSchema {

  val test = DHTTable(0, "test")
  val shareInit = DHTTable(1, "utl_share_init")
  val shareCount = DHTTable(2, "utl_share_count")
  val auth = DHTTable(3, "utl_auth")
  val votingResult = DHTTable(4, "utl_voting_res")
  val votingUser = DHTTable(5, "utl_voting_user")
  val ratingResult = DHTTable(6, "utl_rating_res")
  val ratingUser = DHTTable(7, "utl_rating_user")
  val syncCookies = DHTTable(8, "utl_sync_cookies")
  val authToUid = DHTTable(9, "utl_auth_to_uid")
  val userProfiles = DHTTable(10, "utl_user_profiles")

  val tables = Seq(
    test,
    shareInit,
    shareCount,
    auth,
    votingResult,
    votingUser,
    ratingResult,
    ratingUser,
    syncCookies,
    authToUid,
    userProfiles
  )

  tables.groupBy(_.tableId).find(_._2.size > 1).map{
    case (id, seq) => throw new RuntimeException(s"duplicate table id $id")
  }

  tables.groupBy(_.name).find(_._2.size > 1).map{
    case (id, seq) => throw new RuntimeException(s"duplicate table name $id")
  }

}

case class DHTTable(tableId: Int, name: String, ttl: Long = 0)
