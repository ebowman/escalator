package com.gilt.aws.escalator

import scala.concurrent.{Future, Await}

object Util {
  implicit class FutureResult[T](val future: Future[T]) extends AnyVal {
    import scala.concurrent.duration._
    def toResult: T = Await.result(future, 240.seconds)
  }
}
