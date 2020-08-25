package stateless

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.util.Try

object Flink_Ex1_Rw extends App {

  Try {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // Get the parameters from args
    val params: ParameterTool = ParameterTool.fromArgs(args)
    // Add parameters to execution environment
    env.getConfig.setGlobalJobParameters(params)

    val host: String = params.get("host")
    val port: String = params.get("port")
    val outputPath: String = params.get("output")
    var optionInput: Option[DataStream[String]] = None

    /// TEXT INPUT
    if (params.has("input")) {
      val inputPath: String = params.get("input")
      val format: TextInputFormat = new TextInputFormat(new Path(inputPath))
      // --input /Users/matthiaswettstein/SynologyDrive/Matthias/Hack/Intellipaat/shakespeare.txt
      // OR: val input: DataStream[String] = env.readFile(format, inputPath, FileProcessingMode.PROCESS_ONCE, 10)
      // or: PROCESS_CONTINUOUSLY
      optionInput = Some(env.readTextFile(inputPath))
    }

    /// SOCKET INPUT
    // make sure nc -l <port> is running beforehand
    if (params.has("host") && params.has("port")) {
      optionInput = Some(env.socketTextStream(params.get("host"), params.get("port").toInt))
    }

    if (optionInput.isEmpty) {
      println("either --input, --host, --port")
      System.exit(1)
    } else {
      val input: DataStream[String] = optionInput.get

      val wordsStream: DataStream[String] = input.map {
        input: String =>
          input.split(" ")
            .map(_.trim)
      }
        .map {
          array: Array[String] =>
            array.collect {
              case word if word.nonEmpty =>
                (word.toLowerCase.replaceAll("[\\W]|_", ""), 1)
            }
        }
        .map {
          array: Array[(String, Int)] =>
            array.mkString(", ")
        }

      wordsStream.print
      wordsStream.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE)

      /*
      val sink: StreamingFileSink[String] = StreamingFileSink
        .forRowFormat(new Path(outputPath), new SimpleStringEncoder[String]("UTF-8"))
        .withRollingPolicy(
          DefaultRollingPolicy.builder()
            .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
            .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
            .withMaxPartSize(1024 * 1024 * 1024)
            .build())
        .build()

      wordsStream.addSink(sink)
       */

      env.execute("Read and write")
    }

  } getOrElse println("No Ar")

}
