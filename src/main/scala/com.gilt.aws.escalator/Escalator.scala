package com.gilt.aws.escalator

import java.nio.ByteBuffer
import java.util.concurrent.ArrayBlockingQueue

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, blocking}

case class Config(endpoint: String = "https://kinesis.us-east-1.amazonaws.com",
                  region: String = sys.env("AWS_REGION"),
                  credentials: AWSCredentials = EnvironmentCredentials)

object EnvironmentCredentials extends AWSCredentials {
  lazy val getAWSAccessKeyId: String = sys.env("AWS_ACCESS_KEY_ID")
  lazy val getAWSSecretKey: String = sys.env("AWS_SECRET_ACCESS_KEY")
}

case class StreamDescription(name: String, arn: String, status: String)

class Escalator(config: Config = Config())(implicit ec: ExecutionContext) {
  val client = new AmazonKinesisClient(config.credentials: AWSCredentials)
  client.setEndpoint(config.endpoint, "kinesis", config.region)

  def createStream(streamName: String, shardCount: Int): Future[Unit] = Future {
    blocking(client.createStream(streamName, shardCount))
  }

  def deleteStream(streamName: String): Future[Unit] = Future {
    client.deleteStream(streamName)
  }

  def listStreams(limit: Int = 10): Stream[Future[Seq[String]]] = {
    val request = new ListStreamsRequest()
    request.setLimit(limit)
    val resultQueue = new ArrayBlockingQueue[ListStreamsResult](1)

    def firstRequest(): Future[Seq[String]] = Future {
      val result = blocking(client.listStreams(request))
      resultQueue.put(result)
      result.getStreamNames.asScala
    }

    def nextRequest(): Stream[Future[Seq[String]]] = {
      val prevResult = resultQueue.take()
      if (prevResult.isHasMoreStreams) {
        Future {
          val names = prevResult.getStreamNames.asScala
          if (names.nonEmpty) {
            request.setExclusiveStartStreamName(names.last)
          }
          val nextResult = blocking(client.listStreams(request))
          resultQueue.put(nextResult)
          nextResult.getStreamNames.asScala
        } #:: nextRequest()
      } else {
        Stream.empty
      }
    }

    firstRequest() #:: nextRequest()
  }

  def containsStream(streamName: String, limit: Int = 10): Future[Boolean] = {
    def contains(futures: Stream[Future[Seq[String]]]): Future[Boolean] = {
      if (futures.isEmpty) Future.successful(false)
      else {
        futures.head.flatMap {
          seq =>
            if (seq.contains(streamName)) Future.successful(true)
            else contains(futures.tail)
        }
      }
    }
    contains(listStreams(limit))
  }

  def shards(streamName: String, limit: Int = 10): Stream[Future[Seq[Shard]]] = {
    val request = new DescribeStreamRequest().withStreamName(streamName).withLimit(limit).withExclusiveStartShardId(null)
    val resultQueue = new ArrayBlockingQueue[DescribeStreamResult](1)

    def firstRequest(): Future[Seq[Shard]] = Future {
      val result = blocking(client.describeStream(request))
      resultQueue.put(result)
      result.getStreamDescription.getShards.asScala
    }

    def nextRequest(): Stream[Future[Seq[Shard]]] = {
      val prevResult = resultQueue.take()
      val prevShards = prevResult.getStreamDescription.getShards.asScala
      if (prevResult.getStreamDescription.getHasMoreShards && (prevShards != null && prevShards.nonEmpty)) {
        Future {
          request.setExclusiveStartShardId(prevShards.last.getShardId)
          val nextResult = blocking(client.describeStream(request))
          resultQueue.put(nextResult)
          nextResult.getStreamDescription.getShards.asScala
        } #:: nextRequest()
      } else {
        Future.successful(Nil) #:: Stream.empty
      }
    }

    firstRequest() #:: nextRequest()
  }

  def findShard(streamName: String, shardId: String, limit: Int = 10): Future[Option[Shard]] = {
    val shrds: Stream[Future[Seq[Shard]]] = shards(streamName, limit)

    def find(s: Stream[Future[Seq[Shard]]]): Future[Option[Shard]] = {
      if (s.isEmpty) Future.successful(None)
      else {
        s.head.flatMap {
          shards =>
            val result = shards.find(_.getShardId == shardId)
            if (result == None) find(s.tail)
            else Future.successful(result)
        }
      }
    }

    find(shards(streamName))
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

  def getRecords(streamName: String,
                 shard: Shard,
                 shardIterType: ShardIterType,
                 limit: Int
                 ): Future[Stream[Future[Seq[Record]]]] = {

    getShardIterator(streamName, shard, shardIterType) map { shardIterator =>
      val request = new GetRecordsRequest
      request.setLimit(limit)
      val resultQueue = new ArrayBlockingQueue[GetRecordsResult](1)

      def firstRequest(): Future[Stream[Record]] = Future {
        request.setShardIterator(shardIterator)
        val result = blocking(client.getRecords(request))
        resultQueue.put(result)
        result.getRecords.asScala.toStream
      }

      def nextRequest(): Stream[Future[Stream[Record]]] = {
        val prevResult = resultQueue.take()
        val prevRecords = prevResult.getRecords.asScala
        if (prevResult.getNextShardIterator == null) {
          // this means the shard was closed, which probably means it was merged into another shard
          // not sure what happens if it was deleted
          Future.successful(Stream.empty) #:: Stream.empty
        } else {
          Future {
            request.setShardIterator(prevResult.getNextShardIterator)
            val nextResult = blocking(client.getRecords(request))
            resultQueue.put(nextResult)
            nextResult.getRecords.asScala.toStream
          } #:: nextRequest()
        }
      }

      firstRequest() #:: nextRequest()
    }
  }

  // returns (shardId, sequenceNumber)
  def putRecord(streamName: String, data: ByteBuffer, partitionKey: String, sequenceNumber: Option[String] = None): Future[(String, String)] = Future {
    var request = new PutRecordRequest().withStreamName(streamName).withData(data).withPartitionKey(partitionKey)
    request = sequenceNumber.fold(request)(request.withSequenceNumberForOrdering)
    val response = client.putRecord(request)
    (response.getShardId, response.getSequenceNumber)
  }

  // returns (shardId, sequenceNumber)
  def putRecord(streamName: String, data: Array[Byte], partitionKey: String): Future[(String, String)] = {
    putRecord(streamName, ByteBuffer.wrap(data), partitionKey, None)
  }

  // returns (shardId, sequenceNumber)
  def putRecord(streamName: String, data: Array[Byte], partitionKey: String, sequenceNumber: String): Future[(String, String)] = {
    putRecord(streamName, ByteBuffer.wrap(data), partitionKey, Some(sequenceNumber))
  }

  def describeStream(streamName: String, limit: Int = 10): Future[(StreamDescription, Seq[Shard], Stream[Future[Seq[Shard]]])] = {
    val request = new DescribeStreamRequest()
    request.setStreamName(streamName)
    request.setExclusiveStartShardId(null)
    request.setLimit(limit)
    val resultQueue = new ArrayBlockingQueue[DescribeStreamResult](1)

    def firstRequest(): Future[(StreamDescription, Seq[Shard], Stream[Future[Seq[Shard]]])] = Future {
      val result = blocking(client.describeStream(request))
      resultQueue.put(result)
      val desc = StreamDescription(
        result.getStreamDescription.getStreamName,
        result.getStreamDescription.getStreamARN,
        result.getStreamDescription.getStreamStatus)
      (desc, result.getStreamDescription.getShards.asScala, nextRequest())
    }

    def nextRequest(): Stream[Future[Seq[Shard]]] = {
      val prevResult = resultQueue.take()
      val prevShards = prevResult.getStreamDescription.getShards.asScala
      if (prevResult.getStreamDescription.getHasMoreShards && (prevShards != null && prevShards.nonEmpty)) {
        Future {
          request.setExclusiveStartShardId(prevShards.last.getShardId)
          val nextResult = blocking(client.describeStream(request))
          resultQueue.put(nextResult)
          nextResult.getStreamDescription.getShards.asScala
        } #:: nextRequest()
      } else {
        Future.successful(Nil) #:: Stream.empty
      }
    }

    firstRequest()
  }
}
