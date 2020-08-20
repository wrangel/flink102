package udemy.stateful;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import udemy.transformations.ExtractSpecialities;
import udemy.utils.StreamUtil;

import java.util.logging.Logger;

public class    Flink4StreamOfTuples {

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
                .map(new ParseRow())
                .keyBy(tuple -> tuple.f0)
                .reduce(new SumAndCount())
                .map(new Average());

        outStream3.print();

        env.execute("Tuple stream");
    }

    public static class ParseRow implements MapFunction<String, Tuple3<String, Double, Integer>> {
        public Tuple3<String, Double, Integer> map(String input) throws Exception {
            try {
                String[] rowData = input.split(",");
                return new Tuple3<>(
                        rowData[2].trim(),
                        Double.parseDouble(rowData[1]),
                        1
                );
            } catch (Exception e) {
                System.out.println("Exception in ParseRow");
            }
            return null;
        }
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

    // Course, length, area. Objective: Average course length in each area
    public static class SumAndCount implements ReduceFunction<Tuple3<String, Double, Integer>> {
        public Tuple3<String, Double, Integer> reduce (
                Tuple3<String, Double, Integer> cumulative,
                Tuple3<String, Double, Integer> input
        ) {
            return new Tuple3<>(
                    input.f0,
                    cumulative.f1 + input.f1,
                    cumulative.f2 + 1
            );
        }
    }

    public static class Average implements MapFunction<Tuple3<String, Double, Integer>, Tuple2<String, Double>> {
        public Tuple2<String, Double> map(Tuple3<String, Double, Integer> input) {
            return new Tuple2<>(
                    input.f0,
                    input.f1 / input.f2
            );
        }
    }

}
