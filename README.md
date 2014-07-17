Escalator is an AWS Kinesis client library written in Scala.

It’s primary goal is to provide a scala native API to Kinesis. The Kinesis API is fairly complicated; lots of requests “page”, and other libraries try to hide that fact from the user. Unfortunately requests to the Kinesis APIs are rate limited. This is expressed using scala Streams, and if you’re careful you can avoid making any unnecessary AWS requests.

More docs to follow…
