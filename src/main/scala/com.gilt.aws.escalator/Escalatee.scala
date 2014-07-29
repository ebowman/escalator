package com.gilt.aws.escalator

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model._
import play.api.libs.iteratee.{Enumeratee, Enumerator, Iteratee}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, blocking}

class Escalatee(config: Config = Config())(implicit ec: ExecutionContext) {

  private val client = {
    val cl = new AmazonKinesisClient(config.credentials: AWSCredentials)
    cl.setEndpoint(config.endpoint, "kinesis", config.region)
    cl
  }

  def createStream(streamName: String, shardCount: Int): Future[Unit] = Future {
    blocking(client.createStream(streamName, shardCount))
  }

  def deleteStream(streamName: String): Future[Unit] = Future {
    client.deleteStream(streamName)
  }

  def listStreams(limit: Int = 10): Enumerator[String] = {
    val enumerator: Enumerator[Seq[String]] = Enumerator.unfoldM(None: Option[ListStreamsResult]) {
      case Some(prev) if !prev.isHasMoreStreams => Future.successful(None)
      case state =>
        Future {
          val request = new ListStreamsRequest()
          request.setLimit(limit)

          state.foreach { prevResult =>
            val names = prevResult.getStreamNames.asScala
            if (names.nonEmpty) {
              request.setExclusiveStartStreamName(names.last)
            }
          }

          val result = blocking(client.listStreams(request))
          Some((Some(result), result.getStreamNames.asScala))
        }
    }

    enumerator.flatMap(Enumerator.enumerate)
  }

  def containsStream(streamName: String, limit: Int = 10): Future[Boolean] = {
    val withName: Enumeratee[String, String] = Enumeratee.filter[String](_ == streamName)
    val iter: Iteratee[String, Option[String]] = Iteratee.head[String]
    //listStreams(limit).run(withName.transform(iter.map(_.isDefined)))
    listStreams(limit) |>>> withName &>> iter map (_.isDefined)
  }

  def shards(streamName: String, limit: Int = 10): Enumerator[Shard] = {

    val request = new DescribeStreamRequest().withStreamName(streamName).withLimit(limit).withExclusiveStartShardId(null)

    val enumerator: Enumerator[Seq[Shard]] = Enumerator.unfoldM(None: Option[DescribeStreamResult]) {
      case None => Future {
        val result = blocking(client.describeStream(request))
        result.getStreamDescription.getShards.asScala
        Some((Some(result), result.getStreamDescription.getShards.asScala))
      }
      case Some(prevResult) if prevResult.getStreamDescription.getHasMoreShards => Future {
        request.setExclusiveStartShardId(prevResult.getStreamDescription.getShards.asScala.last.getShardId)
        val nextResult = blocking(client.describeStream(request))
        Some(Some(nextResult), nextResult.getStreamDescription.getShards.asScala)
      }
      case Some(prevResult) => Future.successful(None)
    }

    enumerator.flatMap(Enumerator.enumerate)
  }

  def findShard(streamName: String, shardId: String, limit: Int = 10): Future[Option[Shard]] = {
    val withShardId: Enumeratee[Shard, Shard] = Enumeratee.filter[Shard](_.getShardId == shardId)
    shards(streamName, limit).run(withShardId.transform(Iteratee.head[Shard]))
  }

  def getShardIterator(streamName: String, shard: Shard, shardIterType: ShardIterType): Future[String] = Future {
    val request = new GetShardIteratorRequest()
    request.setStreamName(streamName)
    request.setShardId(shard.getShardId)
    request.setShardIteratorType(shardIterType.underlying)
    shardIterType match {
      case Latest | TrimHorizon => ()
      case AfterSequenceNumber(sequenceNumber) =>
        request.setStartingSequenceNumber(sequenceNumber)
      case AtSequenceNumber(sequenceNumber) =>
        request.setStartingSequenceNumber(sequenceNumber)
    }
    blocking(client.getShardIterator(request)).getShardIterator
  }
}
