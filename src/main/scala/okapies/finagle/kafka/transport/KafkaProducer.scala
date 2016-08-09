package okapies.finagle.kafka.transport

import java.net.{InetSocketAddress, SocketAddress}
import java.security.cert.Certificate

import com.twitter.finagle.Status
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, Promise, Time}
import kafka.producer.Producer
import kafka.producer.KeyedMessage

class KafkaProducer(producer: Producer[String,Array[Byte]])
 extends Transport[KeyedMessage[String,Array[Byte]], Unit] {

  override def remoteAddress: SocketAddress = new InetSocketAddress(0)

  override def peerCertificate: Option[Certificate] = None

  override def localAddress: SocketAddress = new InetSocketAddress(0)

  override def status: Status = Status.Open

  override val onClose: Future[Throwable] = Promise()

  override def close(deadline: Time): Future[Unit] = Future(producer.close)

  override def write(req: KeyedMessage[String,Array[Byte]]): Future[Unit] = Future(producer.send(req))

  override def read(): Future[Unit] = Future(())
}
