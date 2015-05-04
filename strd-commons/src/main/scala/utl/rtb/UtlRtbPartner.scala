package utl.rtb



/**
 *
 */
object UtlRtbPartner extends Enumeration {

  /**
   *
   * @param id
   * @param name
   * @param useInternalMatchingTable use UtlMatchingTable for partenerUID instead of Matching Partner Table
   * @see FrontendServer
   */
  class Partner(id: Int, name: String, val useInternalMatchingTable : Boolean = false) extends Val(id, name)

  type TYPE = Partner


  val ADCAMP        = new Partner(5, "adc", true)



  private val valsById = values.toSeq.map(x=>x.id -> x.asInstanceOf[Partner]).toMap
  private val valsByName = values.toSeq.map(x=>x.toString -> x.asInstanceOf[Partner]).toMap

  def forId(id: Int) : Option[Partner] = valsById.get(id)
  def forName(name: String) : Option[Partner] = valsByName.get(name)


}

