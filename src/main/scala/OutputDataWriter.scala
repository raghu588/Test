import org.apache.kafka.clients.producer.KafkaProducer
import java.util.Properties

import com.fasterxml.jackson.databind.ser.std.NumberSerializers.LongSerializer
import org.apache.kafka.clients.producer._

object OutputDataWriter {

  val props = new Properties()
  //props.put("bootstrap.servers", "10.1.51.29:6667")
  props.put("bootstrap.servers", "10.1.51.27:6667,10.1.51.28:6667,10.1.51.29:6667")
  props.put("client.id", "MarketProducer")
  props.put("acks","1")
  props.put("batch.size", "16384")
  props.put("buffer.memory", "33554432")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  //props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)



  /**
    * This method will writes the data to Kafka.
    * @param data
    */
  def writeToKafka(data:List[String],topicName:String)= {



    try {

      val TOPIC = topicName

      data.foreach { row =>

        val record = new ProducerRecord[String, String](TOPIC, "key1", row)
        val x = producer.send(record)
       println( x.get())
        // println("......"+x.get().topic())

        //println(">>>>>>>>>>>>>>>...")
      }


    } catch {

      case ex: Throwable => ex.printStackTrace() //logger.error("Exception occured in reading csv"+filePath,ex)

    } finally {
      if (producer != null) {
       // producer.close()
      }
    }

  }


}
