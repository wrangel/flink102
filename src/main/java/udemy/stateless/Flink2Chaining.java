package udemy.stateless;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import udemy.utils.StreamUtil;

public class Flink2Chaining {

    public static void main( String[] args ) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> outStream = StreamUtil.getDataStream(env, params)
                .filter(new Filter())
                .map(new CleanString());

        outStream.print();

        env.execute("Filter");

    }

    public static class Filter implements FilterFunction<String> {
        public boolean filter(String input) {
            try {
                Double.parseDouble(input.trim());
                return false;
            }   catch (Exception ignored) {
            }
            return input.length() > 3;
        }
    }

    public static class CleanString implements MapFunction<String, String> {
        public String map(String input) throws Exception {
            return input.trim().toLowerCase();
        }
    }

}
