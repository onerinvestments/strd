package strd.net.http

import io.netty.handler.codec.http.HttpMethod

/**
 * @author Kirill chEbba Chebunin
 */
package object matching {
  object -> {
    def unapply(req: HttpReq): Option[(HttpMethod, String)] = Some(req.method -> req.path)
  }

  object /: {
    def unapply(p: String): Option[(String, String)] = p.split("/").toList match {
        case x :: y => Some(x, y.mkString("/"))
        case _ => None
    }
  }

  object / {
    def unapply(s: String): Option[(String, String)] = {
      val slash = s.lastIndexOf("/")
      if (slash > 0 && slash < s.size - 1) {
        Some(s.substring(0, slash), s.substring(slash + 1))
      } else {
        None
      }
    }
  }

  object :? {
    def unapply(req: HttpReq): Option[(HttpReq, MultiStringMap)] = Some(req, req.query)
  }
  object & {
    def unapply(req: HttpReq): Option[(HttpReq, MultiStringMap)] = Some(req, req.query)
  }

//  abstract class ParamMatchers(val name: String) {
//    def str = new ParamMatcher(name) {}
//    def int = new IntMatcher(name) {}
//  }
//
//  object p extends Dynamic {
//    def selectDynamic(name: String): ParamMatchers = new ParamMatchers(name) {}
//  }
}
