package okapies.finagle.kafka.transport

import java.net.{InetSocketAddress, SocketAddress}
import java.util.Properties

import com.twitter.finagle.Stack.Params
import com.twitter.finagle._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.client.{StackClient, StdStackClient, Transporter}
import com.twitter.finagle.dispatch.{SerialClientDispatcher, SerialServerDispatcher}
import com.twitter.finagle.server.{Listener, StackServer, StdStackServer}
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Closable, Future}
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector}
import kafka.message.MessageAndMetadata
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

/**
  * Created by lucascs on 8/9/16.
  */
case class KafkaServer(stack: Stack[ServiceFactory[MessageAndMetadata[String,String], Any]] = StackServer.newStack[MessageAndMetadata[String,String], Any],
                       params: Params = StackServer.defaultParams) extends StdStackServer[MessageAndMetadata[String,String],Any,KafkaServer] {
  type Rep = MessageAndMetadata[String,String]
  type Req = Any
  override type In = Req

  override protected def newListener(): Listener[In, Out] = new Listener[In,Out] {
    override def listen(addr: SocketAddress)(serveTransport: (Transport[In, Out]) => Unit): ListeningServer = {
      val props: Properties = new Properties()
      props.setProperty("zookeeper.connect", addr match {
        case a:InetSocketAddress => s"${a.getHostName}:${a.getPort}"
      })
      props.setProperty("group.id", "ZUEIRO")
      val consumer: ConsumerConnector = Consumer.create(new ConsumerConfig(props))
      serveTransport(new KafkaConsumer(consumer, "ZUEIRA"))
      NullServer
    }
  }

  override protected def newDispatcher(transport: Transport[In, Out], service: Service[Rep, Any]): Closable =
    new SerialServerDispatcher(transport, service)

  override type Out = Rep

  override protected def copy1(stack: Stack[ServiceFactory[MessageAndMetadata[String, String], Any]] = this.stack, params: Params = this.params): KafkaServer = copy(stack, params)
}

case class KafkaClient(stack: Stack[ServiceFactory[KeyedMessage[String, Array[Byte]], Unit]] = StackClient.newStack[KeyedMessage[String, Array[Byte]], Unit],
                       params: Params = StackClient.defaultParams) extends StdStackClient[KeyedMessage[String,Array[Byte]], Unit, KafkaClient] {
  override type In = KeyedMessage[String, Array[Byte]]

  override protected def copy1(stack: Stack[ServiceFactory[KeyedMessage[String, Array[Byte]], Unit]] = this.stack,
                               params: Params = this.params): KafkaClient =
    copy(stack, params)

  override protected def newDispatcher(transport: Transport[In, Out]): Service[KeyedMessage[String, Array[Byte]], Unit] = {
    val param.Stats(receiver) = params[param.Stats]
    new SerialClientDispatcher(transport, receiver)
  }

  override protected def newTransporter(): Transporter[In, Out] = new Transporter[In,Out] {
    override def apply(addr: SocketAddress): Future[Transport[KeyedMessage[String, Array[Byte]], Unit]] = {
      val props:Properties = new Properties()
      addr match {
        case a:InetSocketAddress =>
          println(s"hoho $a")
          props.setProperty("metadata.broker.list", s"${a.getHostName}:${a.getPort}")
          props.setProperty("serializer.class",         "kafka.serializer.DefaultEncoder")
           props.setProperty("key.serializer.class",     "kafka.serializer.StringEncoder")
           props.setProperty("partitioner.class",        "kafka.producer.DefaultPartitioner")
           props.setProperty("request.required.acks",    "1")
           props.setProperty("message.send.max.retries", "5")
           props.setProperty("retry.backoff.ms",         "500")
      }
      Future {
        val producer:Producer[String,Array[Byte]] = new Producer(new ProducerConfig(props))
        new KafkaProducer(producer)
      }
    }
  }

  override type Out = Unit
}

object KafkaServer {
  def server = KafkaServer()

  def client = ClientBuilder().stack(KafkaClient())
      .hosts("localhost:9092")
    .name("Daora")
    .build()

  def builder = ServerBuilder()
    .stack(server)
    .name("Zoado")
      .bindTo(new InetSocketAddress("localhost", 2181))
    .build(Service.mk[MessageAndMetadata[String,String], Any](x => Future(println(x))))
}