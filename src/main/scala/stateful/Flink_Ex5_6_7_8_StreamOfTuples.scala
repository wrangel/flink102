package stateful

import org.apache.flink.api.common.functions.{FlatMapFunction, ReduceFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import utils.{EnvUtil, StreamUtil}

object Flink_Ex5_6_7_8_StreamOfTuples extends App {

  val (env: StreamExecutionEnvironment, params: ParameterTool) = EnvUtil.getEnv(args)

  val inputStream: DataStream[String] = StreamUtil.getDataStream(env, params)

  /*
  val outputStream1: DataStream[(String, Integer)] = extractSpecialities(inputStream)
    // flatMap needs to take the detour over an object
    .flatMap(FlattenSpecialities)
    .keyBy(_._1) // Stateful operation on each key
    .sum(1);

  val outputStream2: DataStream[String] = inputStream.map {
    line: String =>
      line.split(",")
        .map(_ (0))
        .mkString("")
  }

  val outputStream3: DataStream[(String, Double)] = inputStream.map {
    line: String =>
      val splitLine: Array[String] = line.split(" ")
      (splitLine(0), splitLine(1).toDouble)
  }
    .keyBy(_._1)
    .min(1);
   */

  // Course, length, area. Objective: Average course length in each area
  val outputStream4: DataStream[(String, Double, Int)] = inputStream.map {
    line: String =>
      val splitLine: Array[String] = line.split(",")
      (splitLine(2).trim, splitLine(1).toDouble, 1)
  }
    .keyBy(_._1)
  ////.reduce(SumAndCount) //// giving up with reduce, due to few docs around


  // Write to flink log
  outputStream4.print

  env.execute("Map FlatMap")
  
  object FlattenSpecialities extends FlatMapFunction[String, (String, Integer)] {
    override def flatMap(input: String, out: Collector[(String, Integer)]): Unit = {
      input.split("\t") foreach {
        speciality: String =>
          out.collect((speciality.trim, 1))
      }
    }
  }

  object SumAndCount extends ReduceFunction[(String, Double, Integer)] {
    override def reduce(
                         cumulative: (String, Double, Integer),
                         input: (String, Double, Integer)
                       ): (String, Double, Integer) = {
      (input._1, cumulative._2 + input._2, cumulative._3 + 1)
    }
  }

}
