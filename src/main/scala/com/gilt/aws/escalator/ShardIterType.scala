package com.gilt.aws.escalator

import com.amazonaws.services.kinesis.model.ShardIteratorType

sealed abstract class ShardIterType(val underlying: ShardIteratorType)

case object TrimHorizon extends ShardIterType(ShardIteratorType.TRIM_HORIZON)

case object Latest extends ShardIterType(ShardIteratorType.LATEST)

case class AtSequenceNumber(sequenceNumber: String) extends ShardIterType(ShardIteratorType.AT_SEQUENCE_NUMBER)

case class AfterSequenceNumber(sequenceNumber: String) extends ShardIterType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)

object ShardIterType {

  import com.gilt.aws.escalator

  def TrimHorizon: ShardIterType = escalator.TrimHorizon

  def Latest: ShardIterType = escalator.Latest

  def AtSequenceNumber(sequenceNumber: String): ShardIterType = escalator.AtSequenceNumber(sequenceNumber)

  def AfterSequenceNumber(sequenceNumber: String): ShardIterType = escalator.AfterSequenceNumber(sequenceNumber)
}
