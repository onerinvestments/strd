package strd.dht3

import com.google.protobuf.Message
import org.slf4j.Logger
import strd.concurrent.Promise
import strd.dht._
import strd.dht3.proto.StrdDhtCluster._
import strd.trace.PoolContext
import strd.util.Stats2

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 *
 * User: light
 * Date: 21/04/14
 * Time: 12:25
 */

trait Dht3ClientApi {
  def log : Logger

  def defaultR     : Int
  def defaultW     : Int
  def replicaCount : Int

  def nodes: Seq[Int]
  def ringLookup( key : BIN ) : Seq[Int]

  def requestNode(id : Int, req : DhtDynamicRequest, timeout : Long ) : Future[Message]
  def notifyNode( id : Int, req : DhtDynamicRequest ) : Unit



  class Request(key      : Array[Byte],
                request  : DhtDynamicRequest,
                W        : Int   = defaultW,
                timeout  : Long  = 3000)(implicit executionContext : ExecutionContext) {

    var successed : Int = 0
    var completed : Int = 0
    var exception : Option[NodeFailed] = None

//    val mutex = new Object()
    var last : Option[Message] = None

    val p = Promise[Message]()

    def execute() = {
      val nodes = ringLookup(key)

      Stats2.incr(s"dht3/putTable/${request.getTableId}")

      if (nodes.size < W) {
        p.tryFailure(new DHTReplicaNotEnoughException(s"W=$W, avail=${nodes.size} "))
      } else {
        val toReplicate = nodes.take( math.max( replicaCount, W) )
        val sentCount = toReplicate.size

        PoolContext.traceOutput(4){
          s"""
             |DHT_PUT on nodes: ${toReplicate.mkString(":")}
           """.stripMargin
        }

        toReplicate.map( n => requestNode(n, request, timeout).onComplete{
          case Success(x)  =>

            Request.this.synchronized {
              completed += 1
              successed += 1
              last = Some(x)
              if (completed == sentCount && ! p.isCompleted) {
                if (successed >= W) {
                  p.trySuccess( x )
                } else {
                  exception.map { x =>
                      p.tryFailure(new DHTReplicaNotEnoughException(s"W=$W, total=$sentCount, success=$successed, lastFailedNode: ${x.nodeId}", x.th))
                  }.getOrElse{
                    p.tryFailure(new DHTReplicaNotEnoughException(s"W=$W, total=$sentCount, success=$successed, lastFailedNode: UNKNOWN STATE"))
                  }
                }
              }
            }

          case Failure(th) =>
            Request.this.synchronized {
              exception = Some(NodeFailed(th, n))
              completed += 1

              if (completed == sentCount && !p.isCompleted) {
                if (successed >= W) {
                  p.trySuccess(last.get)
                } else {
                  p.tryFailure(new DHTReplicaNotEnoughException(s"W=$W, total=$sentCount, success=$successed", th))
                }
              }
            }
        } )
      }

      p.future
    }
  }


  def sendPutRequest(key      : Array[Byte],
                     request  : DhtDynamicRequest,
                     W        : Int   = defaultW,
                     timeout  : Long  = 3000)(implicit executionContext : ExecutionContext) : Future[Message] = {

    val req = new Request(key, request, W, timeout)
    req.execute()
  }

}

case class NodeFailed(th:Throwable, nodeId: Int)
case class MergeResult[X]( state : X, hasConflicts : Boolean )
