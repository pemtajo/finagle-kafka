package okapies.finagle.kafka.transport

import java.net.{InetSocketAddress, SocketAddress}
import java.security.cert.Certificate
import java.util.concurrent.Executors

import com.twitter.finagle.Status
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, FuturePool, Promise, Time}
import kafka.consumer.{ConsumerIterator, KafkaStream}
import kafka.consumer.ConsumerConnector
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder

class KafkaConsumer[K,V](consumer: ConsumerConnector, topic: String, keyDecoder: Decoder[K], valueDecoder: Decoder[V])
 extends Transport[Any, MessageAndMetadata[K,V]] {
  val futurePool = FuturePool(Executors.newFixedThreadPool(1))

  val msgs: KafkaStream[K,V] = consumer.createMessageStreams(Map(topic -> 1), keyDecoder, valueDecoder)(topic).head

  val iterator: ConsumerIterator[K,V] = msgs.iterator()

  override def write(req: Any): Future[Unit] = Future(req)

  override def remoteAddress: SocketAddress = new InetSocketAddress(0)

  override def peerCertificate: Option[Certificate] = None

  override def localAddress: SocketAddress = new InetSocketAddress(0)

  override def status: Status = Status.Open

  override def read(): Future[MessageAndMetadata[K, V]] =
    futurePool {
      iterator.next()
    }

  override val onClose: Future[Throwable] = Promise()

  override def close(deadline: Time): Future[Unit] = Future(consumer.shutdown())
}
