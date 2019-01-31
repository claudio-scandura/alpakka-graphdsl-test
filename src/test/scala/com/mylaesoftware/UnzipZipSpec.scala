package com.mylaesoftware

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.Supervision.resumingDecider
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source, Unzip, Zip}
import akka.{Done, NotUsed}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.{Matchers, WordSpec}

class UnzipZipSpec extends WordSpec
  with EmbeddedKafka
  with Matchers
  with Eventually
  with IntegrationPatience
  with ScalaFutures {


  def buildGraph[T](bootstrapServers: String, topic: String, bizzFlow: Flow[CommittableMessage[String, String], T, NotUsed])
                   (implicit system: ActorSystem) = {
    val settings = ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
      .withBootstrapServers(bootstrapServers)
      .withGroupId(UUID.randomUUID().toString)
      .withProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")

    def flow(): Flow[(CommittableMessage[String, String], CommittableOffset), CommittableOffset, NotUsed] = {
      Flow.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._

        val unzip = b.add(Unzip[CommittableMessage[String, String], CommittableOffset]())
        val zip = b.add(Zip[T, CommittableOffset]())

        unzip.out0.via(bizzFlow) ~> zip.in0

        unzip.out1 ~> zip.in1

        FlowShape(unzip.in, zip.out)
      }).map(_._2).withAttributes(supervisionStrategy(resumingDecider))
    }

    Consumer.committableSource(settings, Subscriptions.topics(topic))
      .map(cm => (cm, cm.committableOffset))
      .via(flow())
      .mapAsync(1)(_.commitScaladsl())
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.ignore)(Keep.both)
  }

  "The kafka stream graph" should {

    "ignore error and keep processing coming messages" in {

      implicit val kafkaConfig = EmbeddedKafkaConfig()

      withRunningKafka {
        val topic = s"testTopic-${UUID.randomUUID()}"
        createCustomTopic(topic)

        implicit val sys = ActorSystem("Test-system")
        implicit val mat = ActorMaterializer.create(ActorSystem("Test-system"))

        val result = new AtomicReference[Boolean](false)
        val failingFlow = Flow[CommittableMessage[String, String]].map {
          case cm if cm.record.value() == "fail" => throw new RuntimeException("Error!")
          case _ =>
            result.set(true)
            Done
        }

        val (switch, materialized) = buildGraph(s"localhost:${kafkaConfig.kafkaPort}", topic, failingFlow).run()

        //send failing message
        publishStringMessageToKafka(topic, "fail")

        //send other message
        publishStringMessageToKafka(topic, "other")

        //if second message got through then result should hold `true` flag
        eventually {
          result.get() shouldBe true
        }

        switch.shutdown()
        materialized.futureValue shouldBe Done

        sys.terminate().futureValue
      }
    }
  }

  "The stream graph" should {

    "ignore error and keep processing coming elements" in {

      implicit val sys = ActorSystem("Test-system")
      implicit val mat = ActorMaterializer.create(ActorSystem("Test-system"))

      val result = new AtomicReference[Boolean](false)

      val failingFlow: Flow[String, Done, NotUsed] = Flow[String].map {
        case "b" => throw new RuntimeException("Error!")
        case "c" =>
          result.set(true)
          Done
        case _ => Done
      }

      def buildFlow() =
        Flow.fromGraph(GraphDSL.create() { implicit b =>
          import GraphDSL.Implicits._

          val unzip = b.add(Unzip[String, Int]())
          val zip = b.add(Zip[Done, Int]())

          unzip.out0.via(failingFlow) ~> zip.in0

          unzip.out1 ~> zip.in1

          FlowShape(unzip.in, zip.out)
        }).map(_._2).withAttributes(supervisionStrategy(resumingDecider))


      Source(List("a" -> 1, "b" -> 2, "c" -> 3))
        .via(buildFlow())
        .toMat(Sink.ignore)(Keep.right)
        .run()
        .futureValue

      result.get() shouldBe true
    }
  }

}
