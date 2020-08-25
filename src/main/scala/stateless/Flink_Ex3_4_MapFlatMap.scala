package stateless

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import utils.Transformations.extractSpecialities
import utils.{EnvUtil, StreamUtil}

// --input /Users/matthiaswettstein/sandbox/material/specialities.txt
object Flink_Ex3_4_MapFlatMap extends App {

  val (env: StreamExecutionEnvironment, params: ParameterTool) = EnvUtil.getEnv(args)

  val inputStream: DataStream[String] = StreamUtil.getDataStream(env, params)

  val outputStream: DataStream[String] = extractSpecialities(inputStream)
    // flatMap needs to take the detour over an object
    .flatMap(FlattenSpecialities)

  // Write to flink log
  outputStream.print

  env.execute("Map FlatMap")

  object FlattenSpecialities extends FlatMapFunction[String, String] {

    override def flatMap(input: String, out: Collector[String]): Unit = {
      input.split("\t") foreach {
        speciality: String =>
          out.collect(speciality.trim)
      }
    }

  }

}
