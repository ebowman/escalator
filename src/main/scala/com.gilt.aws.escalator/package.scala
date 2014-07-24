package com.gilt.aws

import com.amazonaws.services.kinesis.model.Shard

package object escalator {

  implicit class RichShard(val shard: Shard) extends AnyVal {
    def isOpen = shard.getSequenceNumberRange == null || shard.getSequenceNumberRange.getEndingSequenceNumber == null

    def canMergeWith(shard2: Shard): Boolean = {
      isOpen && ((getHashRange, shard2.getHashRange) match {
        case ((l1, h1), (l2, h2)) if h1 + 1 == l2 => true
        case ((l1, h1), (l2, h2)) if h2 + 1 == l1 => true
        case _ => false
      })
    }

    def getHashRange: (BigInt, BigInt) = {
      val range = shard.getHashKeyRange
      (BigInt(range.getStartingHashKey), BigInt(range.getEndingHashKey))
    }
  }
}
