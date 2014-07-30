package com.gilt.aws.escalator

import com.amazonaws.auth.AWSCredentials

case class Config(endpoint: String = "https://kinesis.us-east-1.amazonaws.com",
                  region: String = sys.env("AWS_REGION"),
                  credentials: AWSCredentials = EnvironmentCredentials)

object EnvironmentCredentials extends AWSCredentials {
  lazy val getAWSAccessKeyId: String = sys.env("AWS_ACCESS_KEY_ID")
  lazy val getAWSSecretKey: String = sys.env("AWS_SECRET_ACCESS_KEY")
}

case class StreamDescription(name: String, arn: String, status: String)
