package strd.rmi

import com.escalatesoft.subcut.inject.{BindingModule, Injectable}
import org.jboss.netty.buffer.{ChannelBufferInputStream, ChannelBufferOutputStream, ChannelBuffers}
import org.slf4j.LoggerFactory
import org.springframework.remoting.rmi.RemoteInvocationSerializingExporter
import strd.common.Request
import utl.core.http._

import scala.concurrent.ExecutionContext

/**
 *
 * Created by penkov on 21.11.14.
 */
class RemoteExporter extends RemoteInvocationSerializingExporter {
  val log = LoggerFactory.getLogger(getClass)

  def handle(req: MessageRequest) = {
    val ois = createObjectInputStream(new ChannelBufferInputStream(req.req.getContent))
    val invocation = doReadRemoteInvocation(ois)

    if (invocation.getArguments.length == 1 && invocation.getArguments()(0).isInstanceOf[Request]) {
      val r : Request = invocation.getArguments()(0).asInstanceOf[Request]
      log.trace(s"Request from profileId=${r.auth.profileId} (ip=${r.auth.ip}) -> ${req.request.getPath}.${invocation.getMethodName}")
    } else {
      log.trace(invocation.toString)
    }

    val result = invokeAndCreateResult(invocation, getProxy)
    if (result.getException != null) {
      log.error("RMI invocation failed with exception: " + result.getException.getMessage, result.getException)
    }

    val os = new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer())

    doWriteRemoteInvocationResult(result, createObjectOutputStream(os))

    MessageResponse(None,
      contentType = getContentType,
      raw = Some(os.buffer())
    )
  }
}


abstract class RmiController(implicit val bindingModule: BindingModule) extends Router with Injectable {
  implicit val execctx = inject[ExecutionContext]


  def service[A](implicit m: Manifest[A]) = {
    val service = inject[A]

    val clazz = m.runtimeClass

    clazz.getSimpleName -> {
      val e = new RemoteExporter
      e.setService(service)
      e.setServiceInterface(clazz)
      e.prepare()
      e
    }
  }

  val routes: List[(Method, Router.Handler)]
}
