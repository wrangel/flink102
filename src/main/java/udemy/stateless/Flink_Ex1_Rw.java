package udemy.stateless;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

// --input <someFile.txt>  or --host localhost --port 8081 (socket (nc -l 8081))
public class Flink_Ex1_Rw {

    public static void main(String[] args) throws Exception {

        DataStream<String> input = null;
        // Determines automatically whether env is local or else
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Get the parameters from args
        final ParameterTool params = ParameterTool.fromArgs(args);
        // Add parameters to execution environment
        env.getConfig().setGlobalJobParameters(params);
        if(params.has("input")) {
            input = env.readTextFile(params.get("input"));
        }
        else if(params.has("host") && params.has("port")) {
            input = env.socketTextStream(params.get("host"), Integer.parseInt(params.get("port")));
        }
        else {
            System.out.println("Use --host and --port to specify socket");
            System.out.println("Use --input to specify file input");
        }
        if(input == null) {
            System.exit(1);
        }

        // Write to flink log
        input.print();

        // Write to flat file
        input.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);

        env.execute("Read and write");

    }

}
