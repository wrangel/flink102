package udemy.stateful;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import udemy.transformations.Average;
import udemy.transformations.ParseRow;
import udemy.transformations.SumAndCounts;
import udemy.utils.StreamUtil;

public class Flink_Ex5_6_7_8_StreamOfTuples {

    public static void main( String[] args ) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

        /*
        DataStream<Tuple2<String, Integer>> outStream1 = dataStream
                .map(new ExtractSpecialities())
                .flatMap(new SplitSpecial())
                .keyBy(tuple -> tuple.f0) // Stateful operation on each key
                .sum(1);

        DataStream<Tuple2<String, Double>> outStream2 = dataStream
                .map(new RowSplitter())
                .keyBy(tuple -> tuple.f0)
                .min(1);
         */

        DataStream<Tuple2<String, Double>> outStream3 = dataStream
                .map(new ParseRow.ParseRow6())
                .keyBy(tuple -> tuple.f0)
                .reduce(new SumAndCounts.SumAndCount1())
                .map(new Average());

        outStream3.print();

        env.execute("Tuple stream");
    }

    public static class SplitSpecial implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String input, Collector<Tuple2<String, Integer>> out) throws Exception {
            try {
                String[] specialities = input.split("\t");
                for (String speciality : specialities) {
                    out.collect(new Tuple2<>(speciality.trim(), 1));
                }
            } catch (Exception ignored) {
                System.out.println("Exception in SplitSpecial");
            }
        }
    }

    public static class RowSplitter implements MapFunction<String, Tuple2<String, Double>> {
        public Tuple2<String, Double> map(String row) throws Exception {
            try {
                String[] fields = row.split(" ");
                if(fields.length == 2) {
                    // Name, running time in minutes
                    return new Tuple2<>(fields[0], Double.parseDouble(fields[1]));
                }
            } catch (Exception ignored) {
                System.out.println("Exception in RowSplitter");
            }
            return null;
        }
    }

}
