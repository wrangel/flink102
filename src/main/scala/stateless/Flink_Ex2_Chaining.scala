package stateless

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import utils.{EnvUtil, StreamUtil}

import scala.util.Try

object Flink_Ex2_Chaining extends App {

  val (env: StreamExecutionEnvironment, params: ParameterTool) = EnvUtil.getEnv(args)

  val inputStream: DataStream[String] = StreamUtil.getDataStream(env, params)

  // impossible to combine map and filter to collect due to flink api
  val outputStream: DataStream[String] = inputStream.filter {
    t: String =>
      Try {
        t.toDouble
        t.length > 3
      }
        .getOrElse(false)
  }
    .map {
      t: String =>
        t.trim
          .toLowerCase
    }

  // Alternative: Java enquivalent: explicit function call
  val outputStream2: DataStream[String] = inputStream.filter {
    t: String =>
      explFilter(t)
  }

  def explFilter(t: String): Boolean = {
    Try {
      t.toDouble
      t.length > 3
    }
      .getOrElse(false)
  }


  // Write to flink log
  outputStream.print

  env.execute("Filter")

}
