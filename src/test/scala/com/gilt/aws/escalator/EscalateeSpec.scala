package com.gilt.aws.escalator

import com.amazonaws.services.kinesis.model.Record
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import play.api.libs.iteratee.{Enumeratee, Enumerator, Iteratee}

import scala.concurrent.Future
import scala.util.{Success, Try}

class EscalateeSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  import com.gilt.aws.escalator.Util._

import scala.concurrent.ExecutionContext.Implicits.global

  val client = new Escalatee()
  val streamNames = (1 to 3) map (_ => s"EscalateeSpec_${util.Random.alphanumeric.take(10).mkString}")

  def blockUntilNoneMatch(status: String): Unit = {
    val isSomething: Enumeratee[String, Boolean] = Enumeratee.map {
      streamName: String =>
        Try(client.describeStream(streamName)._1.toResult) match {
          case Success(StreamDescription(_, _, `status`)) => true
          case _ => false
        }
    }
    val isTrue: Enumeratee[Boolean, Boolean] = Enumeratee.filter[Boolean](_ == true)
    val head = Iteratee.head
    var creating = true
    while (creating) {
      val streamEnumerator = client.listStreams()
      (streamEnumerator &> isSomething &> isTrue).run(Iteratee.head).toResult match {
        case None => creating = false
        case _ =>
      }
      if (creating) Thread.sleep(1000)
    }

  }

  override def beforeAll() {
    blockUntilNoneMatch("DELETING")
    // create 3 streams, with 1, 2, & 3 shards each
    Future.sequence(streamNames.zipWithIndex.map(ni => client.createStream(ni._1, ni._2 + 1))).toResult
    blockUntilNoneMatch("CREATING")
  }

  override def afterAll() {
    Future.sequence(streamNames.map(client.deleteStream)).toResult
  }

  "An Escalator" should "page through multiple streams" in {
    val streamEnum: Enumerator[String] = client.listStreams(2)

    val streams = streamEnum.run(Iteratee.getChunks).toResult.toStream

    streamNames.foreach(name => streams should contain(name))
    streamNames.foreach(name => client.containsStream(name, 1).toResult should be(true))
    client.containsStream(util.Random.alphanumeric.take(10).mkString, 1).toResult should be(false)
  }

  it should "support paging through and finding shards" in {
    val shardEnum = client.shards(streamNames.last, 1)
    val shards = shardEnum.run(Iteratee.getChunks).toResult
    shards.size should equal(3)

    client.findShard(streamNames.last, shards.head.getShardId, 1).toResult should equal(Some(shards.head))
    client.findShard(streamNames.last, "", 1).toResult should equal(None)
  }

  it should "describe streams correctly" in {
    val result = client.describeStream(streamNames.last, 1)
    val description = result._1.toResult
    description.name should equal(streamNames.last)
    description.status should equal("ACTIVE")

    val shards = result._2.run(Iteratee.getChunks).toResult
    shards.size should be(3)
  }

  it should "support ordered queuing and unqueuing records" in {
    val recordValues = Seq("record1", "record2", "record3").map(_.getBytes)

    val streamName = streamNames.head

    val firstPut = client.putRecord(streamName, recordValues(0), "partKey").toResult
    val secondPut = client.putRecord(streamName, recordValues(1), "partKey", firstPut._2).toResult
    val thirdPut = client.putRecord(streamName, recordValues(2), "partKey", secondPut._2).toResult

    val shard = client.shards(streamName).run(Iteratee.getChunks).toResult.head

    def records(shardIterType: ShardIterType) = client.getRecords(streamName, shard, shardIterType, recordValues.size)

    val result = (records(TrimHorizon) &> Enumeratee.take(3)).run(Iteratee.getChunks).toResult

    // note order is guaranteed
    result.map(_.getData.array().toIndexedSeq) should equal(recordValues.map(_.toIndexedSeq))

    // read the first two, then read the second again, then pick up after the second
    {
      val firstTwo = (records(TrimHorizon) &> Enumeratee.take(2)).run(Iteratee.getChunks).toResult
      val secondCall =
        (records(AtSequenceNumber(firstTwo.last.getSequenceNumber)) &> Enumeratee.take(1)).run(Iteratee.getChunks)
      secondCall.toResult.head.getData.array().toSeq should equal(recordValues(1).toSeq)
      val thirdCall =
        (records(AfterSequenceNumber(firstTwo.last.getSequenceNumber)) &> Enumeratee.take(1)).run(Iteratee.getChunks)
      thirdCall.toResult.head.getData.array().toSeq should equal(recordValues(2).toSeq)
    }
  }
}
