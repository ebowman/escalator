package com.gilt.aws.escalator

import com.amazonaws.services.kinesis.model.{Record, Shard}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.{Await, Future}
import scala.util.{Success, Try}

class EscalatorSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  import Util._

  import scala.concurrent.ExecutionContext.Implicits.global
  val client = new Escalator()
  val streamNames = (1 to 3) map (_ => s"EscalatorSpec_${util.Random.alphanumeric.take(10).mkString}")

  def blockUntilNoneMatch(status: String): Unit = {
    var waiting = true
    while (waiting) {
      val streams = Future.sequence(client.listStreams()).toResult.flatten
      waiting = streams.exists { stream =>
        Try(client.describeStream(stream).toResult) match {
          case Success((StreamDescription(_, _, `status`), _, _)) => true
          case _ => false
        }
      }
      if (waiting) Thread.sleep(1000)
    }

  }

  override def beforeAll() {
    // create 3 streams, with 1, 2, & 3 shards each
    blockUntilNoneMatch("DELETING")
    Future.sequence(streamNames.zipWithIndex.map(ni => client.createStream(ni._1, ni._2 + 1))).toResult
    blockUntilNoneMatch("CREATING")
  }

  override def afterAll() {
    Future.sequence(streamNames.map(client.deleteStream)).toResult
  }

  "An Escalator" should "page through multiple streams" in {
    val streams = Future.sequence(client.listStreams(2)).map(_.flatten).toResult
    streamNames.foreach(name => streams should contain(name))
    streamNames.foreach(name => client.containsStream(name, 1).toResult should be(true))
    client.containsStream(util.Random.alphanumeric.take(10).mkString, 1).toResult should be(false)
  }

  it should "support paging through and finding shards" in {
    val shards: Seq[Shard] = Future.sequence(client.shards(streamNames.last, 1)).toResult.flatten
    shards.size should equal(3)

    client.findShard(streamNames.last, shards.head.getShardId, 1).toResult should equal(Some(shards.head))
    client.findShard(streamNames.last, "", 1).toResult should equal(None)
  }

  it should "describe streams correctly" in {
    val toResult: (StreamDescription, Seq[Shard], Stream[Future[Seq[Shard]]]) =
      client.describeStream(streamNames.last, 1).toResult
    toResult._1.name should equal(streamNames.last)
    toResult._1.status should equal("ACTIVE")

    val shards = Seq(toResult._2, Future.sequence(toResult._3).toResult.flatten).flatten.toSeq
    shards.size should be (3)
  }

  it should "support ordered queuing and unqueuing records" in {
    val records = Seq("record1", "record2", "record3").map(_.getBytes)

    val firstPut = client.putRecord(streamNames.head, records(0), "partKey").toResult
    val secondPut = client.putRecord(streamNames.head, records(1), "partKey", firstPut._2).toResult
    val thirdPut = client.putRecord(streamNames.head, records(2), "partKey", secondPut._2).toResult

    val shard = Future.sequence(client.shards(streamNames.head)).toResult.flatten.head

    val toResult: Stream[Future[Seq[Record]]] = client.getRecords(streamNames.head, shard, TrimHorizon, records.size).toResult

    // note order is guaranteed
    toResult.map(_.toResult).flatten.take(3).map(_.getData.array()).map(_.toIndexedSeq) should equal(records.map(_.toIndexedSeq))

    // read the first two, then read the second again, then pick up after the second
    {
      val toResult: Stream[Future[Seq[Record]]] = client.getRecords(streamNames.head, shard, TrimHorizon, records.size).toResult
      val firstTwo: Stream[Record] = toResult.map(_.toResult).flatten

      val secondRecord = firstTwo.drop(1).head
      val secondCall: Record = client.getRecords(streamNames.head, shard, AtSequenceNumber(secondRecord.getSequenceNumber), records.size).toResult.head.toResult.head
      secondCall.getData.array().toSeq should equal(records(1).toSeq)
      val thirdCall: Record = client.getRecords(streamNames.head, shard, AfterSequenceNumber(secondRecord.getSequenceNumber), records.size).toResult.head.toResult.head
      thirdCall.getData.array().toSeq should equal(records(2).toSeq)
    }
  }
}
