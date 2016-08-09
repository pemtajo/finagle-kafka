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
import kafka.serializer._

case class Config(prop:Properties) {
  def mk(): (Config, Stack.Param[Config]) =
    (this, Config.param)
}

object Config {
  implicit val param = Stack.Param(Config(new Properties()))
}

case class Topic(topic:Option[String]) {
  def mk(): (Topic, Stack.Param[Topic]) =
    (this, Topic.param)
}

object Topic {
  implicit val param = Stack.Param(Topic(None))
}

case class KeyDecoder(decoder:Decoder[_]) {
  def mk(): (KeyDecoder, Stack.Param[KeyDecoder]) =
    (this, KeyDecoder.param)
}

object KeyDecoder {
  implicit val param = Stack.Param(KeyDecoder(new DefaultDecoder))
}

case class ValueDecoder(decoder:Decoder[_]) {
  def mk(): (ValueDecoder, Stack.Param[ValueDecoder]) =
    (this, ValueDecoder.param)
}

object ValueDecoder {
  implicit val param = Stack.Param(ValueDecoder(new DefaultDecoder))
}

case class KafkaServer[K,V](stack: Stack[ServiceFactory[MessageAndMetadata[K,V], Any]] = StackServer.newStack[MessageAndMetadata[K,V], Any],
                       params: Params =
                       StackServer.defaultParams
                         + Config.param.default
                         + KeyDecoder.param.default
                         + ValueDecoder.param.default
                         + Topic.param.default)
  extends StdStackServer[MessageAndMetadata[K,V],Any,KafkaServer[K,V]] {
  override type In = Any
  override type Out = MessageAndMetadata[K,V]

  override protected def newListener(): Listener[In, Out] = new Listener[In,Out] {
    override def listen(addr: SocketAddress)(serveTransport: (Transport[In, Out]) => Unit): ListeningServer = {
      val Config(props) = params[Config]
      val KeyDecoder(keyDecoder:Decoder[K]) = params[KeyDecoder]
      val ValueDecoder(valueDecoder:Decoder[V]) = params[ValueDecoder]
      val Topic(topic) = params[Topic]
      topic match {
        case Some(topicName) =>
          props.setProperty("zookeeper.connect", addr match {
            case a:InetSocketAddress => s"${a.getHostName}:${a.getPort}"
          })
          val consumer: ConsumerConnector = Consumer.create(new ConsumerConfig(props))
          serveTransport(new KafkaConsumer(consumer, topicName, keyDecoder, valueDecoder))
          NullServer
        case None => throw new IllegalArgumentException("Topic config is required")
      }

    }
  }

  override protected def newDispatcher(transport: Transport[In, Out], service: Service[MessageAndMetadata[K,V], Any]): Closable =
    new SerialServerDispatcher(transport, service)

  override protected def copy1(stack: Stack[ServiceFactory[MessageAndMetadata[K, V], Any]] = this.stack, params: Params = this.params): KafkaServer[K,V] = copy(stack, params)

  def withProperties(props: Map[String, String]): KafkaServer[K,V] = {
    val Config(properties) = params[Config]
    props.foreach { case (k,v) =>  properties.setProperty(k,v)}
    this
  }

  def withKeyDecoder[NK](decoder:Decoder[NK]):KafkaServer[NK,V] = {
    configured(KeyDecoder(decoder)).asInstanceOf[KafkaServer[NK,V]]
  }

  def withValueDecoder[NV](decoder:Decoder[NV]):KafkaServer[K,NV] = {
    configured(ValueDecoder(decoder)).asInstanceOf[KafkaServer[K,NV]]
  }

  def withGroup(group: String, topic: String): KafkaServer[K,V] = {
    params[Config].prop.setProperty("group.id", group)
    configured(Topic(Some(topic)))
  }
}

case class KafkaClient[K,V](stack: Stack[ServiceFactory[KeyedMessage[K,V], Unit]] = StackClient.newStack[KeyedMessage[K,V], Unit],
                       params: Params = StackClient.defaultParams + Config(new Properties))
  extends StdStackClient[KeyedMessage[K,V], Unit, KafkaClient[K,V]] {
  override type In = KeyedMessage[K,V]
  override type Out = Unit

  override protected def copy1(stack: Stack[ServiceFactory[KeyedMessage[K,V], Unit]] = this.stack,
                               params: Params = this.params): KafkaClient[K,V] =
    copy(stack, params)

  override protected def newDispatcher(transport: Transport[In, Out]): Service[KeyedMessage[K,V], Unit] = {
    val param.Stats(receiver) = params[param.Stats]
    new SerialClientDispatcher(transport, receiver)
  }

  override protected def newTransporter(): Transporter[In, Out] = new Transporter[In,Out] {
    override def apply(addr: SocketAddress): Future[Transport[KeyedMessage[K,V], Unit]] = {
      val Config(props) = params[Config]
      addr match {
        case a:InetSocketAddress =>
          props.setProperty("metadata.broker.list", s"${a.getHostName}:${a.getPort}")
      }
      Future {
        val producer:Producer[K,V] = new Producer(new ProducerConfig(props))
        new KafkaProducer(producer)
      }
    }
  }

  def withProperties(props: Map[String, String]): KafkaClient[K,V] = {
    val Config(properties) = params[Config]
    props.foreach { case (k,v) =>  properties.setProperty(k,v)}
    this
  }

  def withKeyEncoder[NK](encoder:Class[_ <: Encoder[NK]]): KafkaClient[NK,V] = {
    withProperties(Map("key.serializer.class" -> encoder.getCanonicalName)).asInstanceOf[KafkaClient[NK,V]]
  }

  def withValueEncoder[NV](encoder:Class[_ <: Encoder[NV]]): KafkaClient[K,NV] = {
    withProperties(Map("serializer.class" -> encoder.getCanonicalName)).asInstanceOf[KafkaClient[K,NV]]
  }
}

object KafkaServer {
  def server = KafkaServer()

  def client = ClientBuilder().stack(KafkaClient().withKeyEncoder(classOf[StringEncoder]).withValueEncoder(classOf[StringEncoder]))
      .hosts("localhost:9092")
    .name("client")
    .build()

  def builder = ServerBuilder()
    .stack(KafkaServer().withGroup("GRUPO", "ZUEIRA").withKeyDecoder(new StringDecoder).withValueDecoder(new StringDecoder))
    .name("server")
      .bindTo(new InetSocketAddress("localhost", 2181))
    .build(Service.mk[MessageAndMetadata[String,String], Any](x => Future(println(x.key -> x.message))))
}