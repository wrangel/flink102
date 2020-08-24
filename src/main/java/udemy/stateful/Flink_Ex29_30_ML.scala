package udemy.stateful

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}

/* input: course (String) (BLANK) course length (String
 */
class Flink_Ex29_30_ML extends App {
  val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  val params = ParameterTool.fromArgs(args)
  env.getConfig.setGlobalJobParameters(params)

  // Get input data
  val data: DataSet[String] = env.readTextFile(params.get("input"))

  val courseLengths = data.map {
    element: String =>
      // Split input line into Array of Strings
      element.toLowerCase.split(" ")
        .map(x => (x.head, x.last.toInt))
  }
    .groupBy(0)
    .sum(1)

  courseLengths.print()
}
