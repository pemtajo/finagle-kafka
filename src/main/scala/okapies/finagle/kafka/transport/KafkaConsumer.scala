package okapies.finagle.kafka.transport

import java.net.{InetSocketAddress, SocketAddress}
import java.security.cert.Certificate
import java.util
import java.util.concurrent.Executors

import com.twitter.finagle.Status
import com.twitter.finagle.transport.Transport
import com.twitter.util._
import kafka.consumer.{ConsumerIterator, KafkaStream}
import kafka.consumer.ConsumerConnector
import kafka.message.MessageAndMetadata
import kafka.producer.KeyedMessage
import kafka.serializer.{DefaultDecoder, StringDecoder}

import scala.collection.JavaConversions._

class KafkaConsumer(consumer:ConsumerConnector, topic:String)
 extends Transport[Any, MessageAndMetadata[String,String]] {
  val futurePool = FuturePool(Executors.newFixedThreadPool(1))

  val msgs: KafkaStream[String, String] = consumer.createMessageStreams(Map(topic -> 1), new StringDecoder, new StringDecoder)(topic).head

  val iterator: ConsumerIterator[String, String] = msgs.iterator()

  override def write(req: Any): Future[Unit] = Future(req)

  override def remoteAddress: SocketAddress = new InetSocketAddress(0)

  override def peerCertificate: Option[Certificate] = None

  override def localAddress: SocketAddress = new InetSocketAddress(0)

  override def status: Status = Status.Open

  override def read(): Future[MessageAndMetadata[String, String]] =
    futurePool {
      iterator.next()
    }

  override val onClose: Future[Throwable] = Promise()

  override def close(deadline: Time): Future[Unit] = Future(consumer.shutdown())
}
