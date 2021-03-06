package controllers

import play.api.mvc._
import scala.concurrent.Future
import com.daumkakao.s2graph.rest.actors.{ KafkaAggregatorActor, Protocol }
import com.daumkakao.s2graph.rest.config.Config
import Protocol._
import org.apache.kafka.clients.producer.ProducerRecord
object PublishController extends Controller {

  import play.api.libs.concurrent.Execution.Implicits._
  import ApplicationController._


  /**
   * never check validation on string. just redirect strings to kafka.
   */
  def publishOnly(service: String) = withHeaderAsync(parse.text) { request =>
    Future {
      if (!Config.IS_WRITE_SERVER) Unauthorized
      
      val strs = request.body.split("\n")
      strs.foreach(str => {
        val keyedMessage = new ProducerRecord[Key, Val](Config.KAFKA_LOG_TOPIC, s"$service\t$str")
        KafkaAggregatorActor.enqueue(Protocol.KafkaMessage(keyedMessage))
      })

      Ok("publish success.\n").withHeaders(CONNECTION -> "Keep-Alive", "Keep-Alive" -> "timeout=10, max=10")
    }
  }

  def publish(topic: String) = publishOnly(topic)
  //  def mutateBulk(topic: String) = Action.async(parse.text) { request =>
  //    EdgeController.mutateAndPublish(Config.KAFKA_LOG_TOPIC, Config.KAFKA_FAIL_TOPIC, request.body).map { result =>
  //      result.withHeaders(CONNECTION -> "Keep-Alive", "Keep-Alive" -> "timeout=10, max=10")
  //    }
  //  }
  def mutateBulk(topic: String) = withHeaderAsync(parse.text) { request =>
    EdgeController.mutateAndPublish(request.body)
  }
}