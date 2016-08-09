package okapies.finagle.kafka.transport

import java.net.{InetSocketAddress, SocketAddress}
import java.util.Properties

import com.twitter.finagle.Stack.Params
import com.twitter.finagle._
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.dispatch.SerialServerDispatcher
import com.twitter.finagle.server.{Listener, StackServer, StdStackServer}
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Closable, Future}
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector}
import kafka.message.MessageAndMetadata

/**
  * Created by lucascs on 8/9/16.
  */
case class KafkaStack(stack: Stack[ServiceFactory[MessageAndMetadata[String,String], Any]] = StackServer.newStack[MessageAndMetadata[String,String], Any],
                      params: Params = StackServer.defaultParams) extends StdStackServer[MessageAndMetadata[String,String],Any,KafkaStack] {
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

  override protected def copy1(stack: Stack[ServiceFactory[MessageAndMetadata[String, String], Any]] = this.stack, params: Params = this.params): KafkaStack = copy(stack, params)
}

object KafkaStack {
  def server = KafkaStack()

  def builder = ServerBuilder()
    .stack(server)
    .name("Zoado")
      .bindTo(new InetSocketAddress("localhost", 2181))
    .build(Service.mk[MessageAndMetadata[String,String], Any](x => Future(println(x))))
}