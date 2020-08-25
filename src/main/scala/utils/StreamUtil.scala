package utils

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object StreamUtil {

  def getDataStream(env: StreamExecutionEnvironment, params: ParameterTool): DataStream[String] = {
    if (!params.has("input") & !(params.has("host") && params.has("port"))) {
      println("either --input, or --host and --port")
      System.exit(1)
    }
    if (params.has("input"))
      env.readTextFile(params.get("input"))
    else
      env.socketTextStream(params.get("host"), params.get("port").toInt)
  }

}
