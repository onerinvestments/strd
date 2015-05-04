package strd.net

import strd.cluster.proto.StrdCluster.{ErrorCode, RFail}

/**
 *
 * User: light
 * Date: 22/04/14
 * Time: 15:33
 */

object RemoteException {
  def apply(r :RFail) = {
    val count = r.getStackTraceCount
    val ste = new Array[StackTraceElement]( count )
    0.until(count).foreach( i => {
      val e = r.getStackTrace(i)
      ste(i) = new StackTraceElement( e.getDeclaringClass, e.getMethodName, e.getFileName, e.getLineNumber)
    })

    val ex = new RemoteException(r.getMsg, r.getErrorCode)
    ex.setStackTrace( ste )
    ex
  }
}

class RemoteException( msg : String, errCode : ErrorCode) extends Exception(msg + "    errCode:"+ errCode){

}
