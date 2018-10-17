
import scala.collection.mutable.ListBuffer

object  FileLineReader extends App {

  val TOPIC_NAME = "TEST652"
  val bufferedSource = io.Source.fromFile("C:\\Users\\raghavendra\\Desktop\\L1.csv")

  var currentRecordNum = 0
  var data:ListBuffer[String] =new ListBuffer[String]();
  for (line <- bufferedSource.getLines) {
   // val cols = line.split(",").map(_.trim)
    currentRecordNum += 1
    data += line
    if(currentRecordNum % 1000 == 0){
      OutputDataWriter.writeToKafka(data.toList,TOPIC_NAME)
      data.clear()
    }


  }

  OutputDataWriter.writeToKafka(data.toList,TOPIC_NAME)
  data.clear()
  bufferedSource.close
}
