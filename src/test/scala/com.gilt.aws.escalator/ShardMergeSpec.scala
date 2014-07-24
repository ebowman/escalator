package com.gilt.aws.escalator

import com.amazonaws.services.kinesis.model.{Record, Shard}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.annotation.tailrec
import scala.concurrent.{Future, Promise, blocking}
import scala.util.{Failure, Success, Try}

class ShardMergeSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  import com.gilt.aws.escalator.Util._

  import scala.concurrent.ExecutionContext.Implicits.global

  val streamNames = (1 to 1).map(i => s"ShardMergeSpec-${util.Random.alphanumeric.take(5).mkString}")

  val client = new Escalator()

  def createStreamAndWait(streamName: String, shardCount: Int): Future[Seq[Shard]] = {
    val f = client.createStream(streamName, shardCount).toResult
    val promise = Promise[Seq[Shard]]()
    Future {
      var done = false
      while (!done) {
        Try(blocking(client.describeStream(streamName, shardCount).toResult)) match {
          case Success((StreamDescription(_, _, "ACTIVE"), shards, _)) =>
            done = true
            promise.success(shards)
          case Success(_) =>
            blocking(Thread.sleep(1000))
          case Failure(ex) =>
            done = true
            promise.failure(ex)
        }
      }
    }

    promise.future
  }

  override def afterAll() {
    Future.sequence(streamNames.map(client.deleteStream)).toResult
  }

  "An Escalator" should "should merge shards" in {
    val streamName = streamNames.head
    val shards: Seq[Shard] = createStreamAndWait(streamName, 2).toResult
    shards.size should equal(2)
    println(client.shards(streamName).head.toResult)

    for (i <- 1 to 10) {
      val data1 = s"record $i shard 1"
      val data2 = s"record $i shard 2"
      val r1 = client.putRecordToShard(streamName, shards(0), data1.getBytes).toResult
      val r2 = client.putRecordToShard(streamName, shards(1), data2.getBytes).toResult
      println((data1, r1))
      println((data2, r2))
    }

    var shard0Stream: Stream[Seq[Record]] = client.getRecords(streamName, shards(0), TrimHorizon, 10).toResult.map(_.toResult)
    var shard1Stream: Stream[Seq[Record]] = client.getRecords(streamName, shards(1), TrimHorizon, 10).toResult.map(_.toResult)

    println(client.shards(streamName).head.toResult)

    client.mergeShards(streamName, shards(0).getShardId, shards(1).getShardId).toResult
    println("merged")

    // if we merge the shards, then both iterators will hit the end, and we need make a new call to talk to the newly
    // merged shard

    shard0Stream.flatten.take(11).toSeq.size should equal(10)
    shard1Stream.flatten.take(11).toSeq.size should equal(10)

    @tailrec
    def recurseUntilEmpty(stream: Stream[_]): Unit = {
      if (stream.isEmpty) ()
      else recurseUntilEmpty(stream.tail)
    }

    // these should guarantee to terminate. Kind of lame if they don't, the test will just
    // hang. Thinking about a better way
    recurseUntilEmpty(shard0Stream)
    recurseUntilEmpty(shard1Stream)

    println(client.shards(streamName).head.toResult)
    //
    //    println(client.shards(streamName).head.toResult)
    //
    //    val shards2 = client.shards(streamName).head.toResult
    //    //println(shards2)
    //    shard0Stream = client.getRecords(streamName, shards2(0), TrimHorizon, 50).toResult.map(_.toResult)
    //
    //    //println(shard0Stream.flatten.take(20))
    //    shard0Stream.flatten.take(21).toSeq.size should equal(20)
  }
}
