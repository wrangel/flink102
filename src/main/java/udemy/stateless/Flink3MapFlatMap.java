package udemy.stateless;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import udemy.transformations.ExtractSpecialities;
import udemy.utils.StreamUtil;

public class Flink3MapFlatMap {

    public static void main( String[] args ) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

        DataStream<String> outStream = dataStream
                .map(new ExtractSpecialities())
                .flatMap(new SplitSpecial());

        outStream.print();

        env.execute("map and flatmap");

    }

    public static class SplitSpecial implements FlatMapFunction<String, String> {
        public void flatMap(String input, Collector<String> out) {
            String[] specialities = input.split("\t");
            for (String speciality : specialities) {
                out.collect(speciality.trim());
            }
        }
    }

}
