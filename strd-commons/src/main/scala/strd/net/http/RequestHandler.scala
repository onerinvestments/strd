package strd.net.http

import io.netty.handler.codec.http.HttpResponseStatus

import scala.concurrent.Future

/**
 * @author Kirill chEbba Chebunin
 */
trait RequestHandler {
  def handle(req: HttpReq): Future[HttpResp]
}

object RequestHandler {
  def apply(handler: (HttpReq) => Future[HttpResp]) = new RequestHandler {
    def handle(req: HttpReq): Future[HttpResp] = handler(req)
  }
}

object DefaultNotFoundHandler extends RequestHandler {
  def handle(req: HttpReq) = {
    Future.successful(HttpResp(
      status = HttpResponseStatus.NOT_FOUND,
      body = Some(Content(req.uri + " is not found"))
    ))
  }
}

trait PartialRequestHandler {
  def tryHandle: PartialFunction[HttpReq, Future[HttpResp]]
}

object PartialRequestHandler {
  def apply(handler: PartialFunction[HttpReq, Future[HttpResp]]) = new PartialRequestHandler {
    def tryHandle = handler
  }
}

trait NestedPartialRequestHandler extends RequestHandler {
  def handlers: Seq[PartialRequestHandler]
  def fallbackHandler: RequestHandler = DefaultNotFoundHandler
  
  def handle(req: HttpReq) = {
    handlers.map(_.tryHandle).find(_.isDefinedAt(req)).map(_.apply(req)).getOrElse {
      fallbackHandler.handle(req)
    }
  }
}

object NestedPartialRequestHandler {

  def apply(partialHandlers: Seq[PartialRequestHandler]): NestedPartialRequestHandler = {
    new NestedPartialRequestHandler {
      def handlers = partialHandlers
    }
  }

  def apply(partialHandlers: Seq[PartialRequestHandler],
            fallback: RequestHandler): NestedPartialRequestHandler = {
    new NestedPartialRequestHandler {
      def handlers = partialHandlers

      override def fallbackHandler = fallback
    }
  }
}
