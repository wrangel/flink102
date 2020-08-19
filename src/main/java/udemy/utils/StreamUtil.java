package udemy.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamUtil {

    public static DataStream<String> getDataStream (StreamExecutionEnvironment env, final ParameterTool params) {
        DataStream<String> dataStream = null;
        if(params.has("input")) {
            dataStream = env.readTextFile(params.get("input"));
        }
        else if(params.has("host") && params.has("port")) {
            dataStream = env.socketTextStream(params.get("host"), Integer.parseInt(params.get("port")));
        }
        else {
            System.out.println("Use --host and --port to specify socket");
            System.out.println("Use --input to specify file input");
        }
        if(dataStream == null) {
            System.exit(1);
        }
        return dataStream;
    }

}
