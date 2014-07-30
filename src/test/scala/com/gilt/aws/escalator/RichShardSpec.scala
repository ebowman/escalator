package com.gilt.aws.escalator

import com.amazonaws.services.kinesis.model.{HashKeyRange, SequenceNumberRange, Shard}
import org.scalatest.{FlatSpec, Matchers}

class RichShardSpec extends FlatSpec with Matchers {
  "A shard" should "report openness" in {
    val openShard = new Shard()
    val openRange = new SequenceNumberRange()
    openRange.setStartingSequenceNumber("0")
    openShard.setSequenceNumberRange(openRange)

    openShard.isOpen should be (true)

    val closedShard = new Shard()
    val closedRange = new SequenceNumberRange
    closedRange.setStartingSequenceNumber("0")
    closedRange.setEndingSequenceNumber("1")
    closedShard.setSequenceNumberRange(closedRange)
    closedShard.isOpen should be (false)
  }
  it should "know when it can merge and when it cannot" in {
    val shardLeft = new Shard
    val leftRange = new HashKeyRange
    leftRange.setStartingHashKey("0")
    leftRange.setEndingHashKey("1")
    shardLeft.setHashKeyRange(leftRange)

    val shardRight = new Shard
    val rightRange = new HashKeyRange
    rightRange.setStartingHashKey("2")
    rightRange.setEndingHashKey("3")
    shardRight.setHashKeyRange(rightRange)

    shardLeft.canMergeWith(shardRight) should be (true)
    shardRight.canMergeWith(shardLeft) should be (true)


    val shardOther = new Shard
    val otherRange = new HashKeyRange
    otherRange.setStartingHashKey("5")
    otherRange.setEndingHashKey("6")
    shardOther.setHashKeyRange(otherRange)

    shardLeft.canMergeWith(shardOther) should be (false)
    shardOther.canMergeWith(shardLeft) should be (false)
    shardRight.canMergeWith(shardOther) should be (false)
    shardOther.canMergeWith(shardRight) should be (false)

  }
}
