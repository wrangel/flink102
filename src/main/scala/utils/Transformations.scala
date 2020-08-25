package utils

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream

object Transformations {

  // Create an object to explicitly express the anonymous function, for re-usability's sake
  def extractSpecialities(input: DataStream[String]): DataStream[String] = {
    input.map {
      line: String =>
        line.split(",")
    }
      // not work: .map(_(1))
      .map {
        a: Array[String] =>
          a(1)
      }
  }

}
