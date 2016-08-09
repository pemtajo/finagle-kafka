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


case class Config(prop:Properties) {
  def mk(): (Config, Stack.Param[Config]) =
    (this, Config.param)
}

object Config {
  implicit val param = Stack.Param(Config(new Properties()))
}

case class KafkaServer(stack: Stack[ServiceFactory[MessageAndMetadata[String,String], Any]] = StackServer.newStack[MessageAndMetadata[String,String], Any],
                       params: Params = StackServer.defaultParams + Config(new Properties)) extends StdStackServer[MessageAndMetadata[String,String],Any,KafkaServer] {
  override type In = Any
  override type Out = MessageAndMetadata[String,String]

  override protected def newListener(): Listener[In, Out] = new Listener[In,Out] {
    override def listen(addr: SocketAddress)(serveTransport: (Transport[In, Out]) => Unit): ListeningServer = {
      val Config(props) = params[Config]
      props.setProperty("zookeeper.connect", addr match {
        case a:InetSocketAddress => s"${a.getHostName}:${a.getPort}"
      })
      val consumer: ConsumerConnector = Consumer.create(new ConsumerConfig(props))
      serveTransport(new KafkaConsumer(consumer, "ZUEIRA"))
      NullServer
    }
  }

  override protected def newDispatcher(transport: Transport[In, Out], service: Service[MessageAndMetadata[String,String], Any]): Closable =
    new SerialServerDispatcher(transport, service)

  override protected def copy1(stack: Stack[ServiceFactory[MessageAndMetadata[String, String], Any]] = this.stack, params: Params = this.params): KafkaServer = copy(stack, params)

  def withProperties(props: Map[String, String]): KafkaServer = {
    val Config(properties) = params[Config]
    props.foreach { case (k,v) =>  properties.setProperty(k,v)}
    this
  }

  def withGroup(group: String): KafkaServer = {
    params[Config].prop.setProperty("group.id", group)
    this
  }
}

case class KafkaClient(stack: Stack[ServiceFactory[KeyedMessage[String, Array[Byte]], Unit]] = StackClient.newStack[KeyedMessage[String, Array[Byte]], Unit],
                       params: Params = StackClient.defaultParams + Config(new Properties)) extends StdStackClient[KeyedMessage[String,Array[Byte]], Unit, KafkaClient] {
  override type In = KeyedMessage[String, Array[Byte]]
  override type Out = Unit

  override protected def copy1(stack: Stack[ServiceFactory[KeyedMessage[String, Array[Byte]], Unit]] = this.stack,
                               params: Params = this.params): KafkaClient =
    copy(stack, params)

  override protected def newDispatcher(transport: Transport[In, Out]): Service[KeyedMessage[String, Array[Byte]], Unit] = {
    val param.Stats(receiver) = params[param.Stats]
    new SerialClientDispatcher(transport, receiver)
  }

  override protected def newTransporter(): Transporter[In, Out] = new Transporter[In,Out] {
    override def apply(addr: SocketAddress): Future[Transport[KeyedMessage[String, Array[Byte]], Unit]] = {
      val Config(props) = params[Config]
      addr match {
        case a:InetSocketAddress =>
          props.setProperty("metadata.broker.list", s"${a.getHostName}:${a.getPort}")
      }
      Future {
        val producer:Producer[String,Array[Byte]] = new Producer(new ProducerConfig(props))
        new KafkaProducer(producer)
      }
    }
  }

  def withProperties(props: Map[String, String]): KafkaClient = {
    val Config(properties) = params[Config]
    props.foreach { case (k,v) =>  properties.setProperty(k,v)}
    this
  }
}

object KafkaServer {
  def server = KafkaServer()

  def client = ClientBuilder().stack(KafkaClient())
      .hosts("localhost:9092")
    .name("client")
    .build()

  def builder = ServerBuilder()
    .stack(server)
    .name("server")
      .bindTo(new InetSocketAddress("localhost", 2181))
    .build(Service.mk[MessageAndMetadata[String,String], Any](x => Future(println(x))))
}