package strd.gateway

import java.text.SimpleDateFormat

import com.escalatesoft.subcut.inject.Injectable
import com.twitter.ostrich.admin.Service
import com.twitter.ostrich.stats.Stats
import lmbrd.zn.util.TimeUtil.TimeGroupType
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.slf4j.LoggerFactory
import play.api.libs.functional.syntax._
import play.api.libs.json._
import strd.client.StrdSender
import strd.cluster.{ClientResponse, RequestFailed, StrdRequest}
import strd.core.StrdTopology
import strd.storage.HostFoldFieldFunction
import strd.util.MonotonicallyIncreaseGenerator
import utl.core.http.{ContentType, MessageResponse, MessageRequest}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 25/04/14
 * Time: 15:46
 */
trait GwEventHandler extends Service with Injectable {

  val log = LoggerFactory.getLogger(getClass)

  val dateFormat = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue() = {
      new SimpleDateFormat("dd.MM.yyyy hh:mm:ss")
    }
  }

  val client = inject[StrdSender]
  implicit val executionContext = inject[ExecutionContext]

  def parse(str: String) = dateFormat.get.parse(str)

  def tg(implicit http : MessageRequest) = http.parameterOption("tg").map(TimeGroupType.valueOf).getOrElse(TimeGroupType.DAY)
  def from(implicit http: MessageRequest) = parse(http.parameter("from")).getTime
  def to(implicit http: MessageRequest) = parse(http.parameter("to")).getTime

  def socType(implicit http: MessageRequest) = http.parameterOption("socType")

  def pidOpt(implicit http: MessageRequest) = http.parameterOption("pid").map(_.toInt)
  def pid(implicit http: MessageRequest) = http.parameter("pid").toInt
  def cid(implicit http: MessageRequest) = http.parameter("cid").toInt
  def advId(implicit http: MessageRequest) = http.parameter("advId").toInt
  def format(implicit http: MessageRequest) = http.parameterOption("format").map{
    case "csv"  => FormatTypes.CSV
    case s:String  => FormatTypes.JSON
  }.getOrElse(FormatTypes.JSON)

  def asc(implicit http: MessageRequest) = http.parameterOption("asc").map(_.toBoolean).getOrElse(true)

  def offset(implicit http: MessageRequest)  = http.parameterOption("offset").map(_.toInt).getOrElse(0)
  def limit(implicit http: MessageRequest)  = http.parameterOption("limit").map(_.toInt).getOrElse(10)


  def count(implicit http: MessageRequest) = http.parameterOption("count").map(_.toInt).getOrElse(50)
  def host(implicit http: MessageRequest) = HostFoldFieldFunction.trimUrl( http.parameter("host") )
  def hostOpt(implicit http: MessageRequest) = http.parameterOption("host").map(HostFoldFieldFunction.trimUrl)


  implicit def projectedToTopologyId(p: (StrdTopology[_, _], StrdTopology[_, _])): String = p._1.topologyId

  implicit def topoToTopologyId(p: StrdTopology[_, _]): String = p.topologyId





  implicit def nodeFormat[T: Format]: Format[Result[T]] =
    ((__ \ "count").format[Int] ~
      (__ \ "entries").format[Seq[T]]
      )(Result.apply, unlift(Result.unapply))


  def processMessage(implicit http: MessageRequest, timeout : Long) : Future[MessageResponse]

  sealed trait FormatType {
  }

  object FormatTypes {
    case object JSON extends FormatType
    case object CSV extends FormatType

  }




  //type FUNCTOR[RESP] = (Seq[RESP]) => Seq[RESP]
  def processReq[RESP](req: Any,
                       topologyId: String,
                       timeout: Long,
                       format : FormatType = FormatTypes.JSON)(implicit tjs: Format[RESP]) = {
    processReq2[RESP, RESP](req, topologyId, timeout,  x => x, format )
  }

  def processReq2[R, RESP](req: Any,
                           topologyId: String,
                           timeout: Long,
                           remap: (Seq[R]) => Seq[RESP],
                           format : FormatType = FormatTypes.JSON )(implicit tjs: Format[RESP]) = {


    format match {
      case FormatTypes.JSON =>
        processReq3[R, RESP](req, topologyId, timeout, remap, x => MessageResponse(Some(Json.toJson(x).toString()), contentType = ContentType.JSON))

      case FormatTypes.CSV =>
        processReq3[R, RESP](req, topologyId, timeout, remap, x => MessageResponse(Some(x.entries.map(e => transformCaseClassToCSV( e ) ).mkString("\n") ), contentType = ContentType.PLAIN))

    }
  }

  def transformCaseClassToCSV( c : Any ) = {
    val str= c.toString
    str.substring(str.indexOf("(") + 1, str.length - 1)
  }


  def processReq3[R, RESP](req: Any,
                           topologyId: String,
                           timeout: Long,
                           remap: (Seq[R]) => Seq[RESP],
                           convert: Result[RESP] => MessageResponse) = {

    sendReq(req, topologyId, timeout, remap) map {
      case Success(res) => convert(res)
      case Failure(e)   => MessageResponse(Some(s"${e.getMessage}"), code = HttpResponseStatus.INTERNAL_SERVER_ERROR)
    }
  }

  def sendReq[R, RESP](req: Any,
                       topologyId: String,
                       timeout: Long,
                       remap: (Seq[R]) => Seq[RESP]) = {
    val r = StrdRequest(MonotonicallyIncreaseGenerator.nextId(), topologyId, req, timeout, 1)
    val startTime = System.currentTimeMillis()
    client.sendRequest(r) map {
      case r: ClientResponse =>
        val timeSpent = (System.currentTimeMillis() - startTime).toInt
        Stats.addMetric(s"req/$topologyId/OK", timeSpent)

        val mappedResponse: Seq[RESP] = remap(r.value.asInstanceOf[Seq[R]])
        val count = if (mappedResponse.size == r.value.size) r.count else mappedResponse.size
        val resp = Result(count, mappedResponse)
        log.debug(f"REQ:$topologyId timeSpent:${timeSpent/1000d}%.3f(s) \n$req")
        Success(resp)

      case f: RequestFailed =>
        val timeSpent = (System.currentTimeMillis() - startTime).toInt
        Stats.incr(s"req/$topologyId/fail/${f.reason}")
        val e = new RuntimeException(s"Request error ${f.reason} :: ${f.message}")
        log.warn(f"REQ_FAILED:$topologyId timeSpent:${timeSpent/1000d}%.3f(s) \n$req\n${e.getMessage}")
        Failure(e)
    }
  }

  def start() = {}

  def shutdown() = {}
}

case class Result[T](count: Int, entries: Seq[T])



