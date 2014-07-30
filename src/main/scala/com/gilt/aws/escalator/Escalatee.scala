package com.gilt.aws.escalator

import java.nio.ByteBuffer

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
    blocking(client.deleteStream(streamName))
  }

  def listStreams(limit: Int = 10): Enumerator[String] = {

    val seqEnumerator = Enumerator.unfoldM[Option[ListStreamsResult], Seq[String]](None) {
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

    for {
      seqResults <- seqEnumerator
      resultEnum <- Enumerator.enumerate(seqResults)
    } yield resultEnum
  }

  def containsStream(streamName: String, limit: Int = 10): Future[Boolean] = {
    val withName = Enumeratee.filter[String](_ == streamName)
    listStreams(limit) |>>> withName &>> Iteratee.head.map(_.isDefined)
  }

  def shards(streamName: String, limit: Int = 10): Enumerator[Shard] = {

    val request = new DescribeStreamRequest().withStreamName(streamName).withLimit(limit).withExclusiveStartShardId(null)

    val enumerator: Enumerator[Seq[Shard]] = Enumerator.unfoldM[Option[DescribeStreamResult], Seq[Shard]](None) {
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

    for {
      seqResults <- enumerator
      resultEnum <- Enumerator.enumerate(seqResults)
    } yield resultEnum
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

  def getRecords(streamName: String, shard: Shard, shardIterType: ShardIterType, limit: Int): Enumerator[Record] = {

    sealed trait EnumeratorState
    case object EnumerationDone extends EnumeratorState
    case class NextIteratorState(shardIter: String) extends EnumeratorState

    def requestNextBatch(shardIterator: String): Future[(Option[EnumeratorState], Seq[Record])] = {
      val request = new GetRecordsRequest
      request.setLimit(limit)
      request.setShardIterator(shardIterator)
      for {
        result <- Future(blocking(client.getRecords(request)))
        nextShardIterator = Option(result.getNextShardIterator)
        records = result.getRecords.asScala
      } yield nextShardIterator match {
        case None => (Some(EnumerationDone), records)
        case Some(shardIter) => (Some(NextIteratorState(shardIter)), records)
      }
    }

    val seqEnumerator = Enumerator.unfoldM[Option[EnumeratorState], Seq[Record]](None) {
      case Some(EnumerationDone) => Future.successful(None)
      case None =>
        for {
          shardIter <- getShardIterator(streamName, shard, shardIterType)
          nextBatch <- requestNextBatch(shardIter)
        } yield Some(nextBatch)
      case Some(NextIteratorState(shardIter)) =>
        for {
          nextBatch <- requestNextBatch(shardIter)
        } yield Some(nextBatch)
    }

    for {
      seqResults <- seqEnumerator
      resultEnum <- Enumerator.enumerate(seqResults)
    } yield resultEnum
  }

  // returns (shardId, sequenceNumber)
  def putRecord(streamName: String, data: ByteBuffer, partitionKey: String, sequenceNumber: Option[String] = None): Future[(String, String)] = Future {
    var request = new PutRecordRequest().withStreamName(streamName).withData(data).withPartitionKey(partitionKey)
    request = sequenceNumber.fold(request)(request.withSequenceNumberForOrdering)
    val response = blocking(client.putRecord(request))
    (response.getShardId, response.getSequenceNumber)
  }

  def putRecordToShard(streamName: String, shard: Shard, data: Array[Byte]): Future[(String, String)] = Future {
    val request = new PutRecordRequest()
    request.setStreamName(streamName)
    request.setPartitionKey(shard.getShardId)
    request.setExplicitHashKey(shard.getHashKeyRange.getStartingHashKey)
    request.setData(ByteBuffer.wrap(data))
    val response = blocking(client.putRecord(request))
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

  def splitShard(streamName: String, shardToSplit: String, startingHashKey: String): Future[Unit] = {
    val request = new SplitShardRequest
    request.setStreamName(streamName)
    request.setShardToSplit(shardToSplit)
    request.setNewStartingHashKey(startingHashKey)
    Future(blocking(client.splitShard(request)))
  }

  def mergeShards(streamName: String, shardToMerge: String, adjacentShard: String): Future[Unit] = {
    val request = new MergeShardsRequest()
    request.setStreamName(streamName)
    request.setShardToMerge(shardToMerge)
    request.setAdjacentShardToMerge(adjacentShard)
    Future(blocking(client.mergeShards(request)))
  }

  def describeStream(streamName: String, limit: Int = 10): (Future[StreamDescription], Enumerator[Shard]) = {

    val request = new DescribeStreamRequest()
    request.setStreamName(streamName)
    request.setExclusiveStartShardId(null)
    request.setLimit(limit)

    val firstResult: Future[DescribeStreamResult] = Future(blocking(client.describeStream(request)))

    val description = firstResult.map {
      result =>
        StreamDescription(
          result.getStreamDescription.getStreamName,
          result.getStreamDescription.getStreamARN,
          result.getStreamDescription.getStreamStatus)
    }


    sealed trait EnumeratorState
    case object EnumerationDone extends EnumeratorState
    case class NextShardId(shardId: String) extends EnumeratorState
    case class FirstResult(firstResult: Future[DescribeStreamResult]) extends EnumeratorState

    def requestNextBatch(startShardId: Option[String]): Future[(EnumeratorState, Seq[Shard])] = {
      startShardId.fold(Future.successful((EnumerationDone: EnumeratorState, Seq.empty[Shard]))) { startShardId =>
        request.setExclusiveStartShardId(startShardId)
        for {
          result <- Future(blocking(client.describeStream(request)))
          desc = result.getStreamDescription
          shards = desc.getShards.asScala.toSeq
          nextShardId = if (desc.getHasMoreShards) Some(shards.last.getShardId) else None
        } yield nextShardId match {
          case None => (EnumerationDone, shards)
          case Some(shardId) => (NextShardId(shardId), shards)
        }
      }
    }

    val seqEnumerator = Enumerator.unfoldM[EnumeratorState, Seq[Shard]](FirstResult(firstResult)) {
      case FirstResult(result) =>
        for {
          r <- result
          desc = r.getStreamDescription
          shards = desc.getShards.asScala.toSeq
          nextShardId = if (desc.getHasMoreShards) Some(shards.last.getShardId) else None
          nextBatch: (EnumeratorState, Seq[Shard]) <- requestNextBatch(nextShardId)
        } yield Some((nextBatch._1, shards ++ nextBatch._2)) // todo: non-optimal
      case NextShardId(shardId) =>
        for {
          nextBatch <- requestNextBatch(Some(shardId))
        } yield Some(nextBatch)
      case EnumerationDone => Future.successful(None)
    }

    (description, for {
      seqResults <- seqEnumerator
      resultEnum <- Enumerator.enumerate(seqResults)
    } yield resultEnum)
  }
}
