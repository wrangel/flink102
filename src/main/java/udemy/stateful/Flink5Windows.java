package udemy.stateful;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.Int;
import udemy.utils.StreamUtil;

public class Flink5Windows {

    public static void main( String[] args ) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

        /*
        // 1) Unkeyed stream
        DataStream<Integer> windowedStream1 = dataStream
                .map(Integer::parseInt)
                // Window Assigner
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                // Evaluation function
                .sum(0);

         */

        // 2) Keyed stream w/ Tumbling window: Window applies to each key separately (requires tuples)
        DataStream<Tuple2<String, Integer>> windowedStream2 = dataStream
                .map(new ParseRow())
                .keyBy(tuple -> tuple.f0)
                // TumblingProcessingTimeWindows is a Window Assigner
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1);

        // 3) Keyed stream w/ Tumbling window: Window applies to each key separately
        DataStream<Tuple2<String, Integer>> windowedStream3 = dataStream
                .map(new ParseRow())
                .keyBy(tuple -> tuple.f0)
                // Two parameters: 1) window size 2) Sliding interval: 1 - 30, 10 - 40, 20 - 50
                .window(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .sum(1);

        /* Anonymous function alternative to parseRow
                 .map(input -> {
                    String[] rowData = input.split(",");
                    return new Tuple2<String, Integer>(
                            rowData[0].trim(),
                            Integer.parseInt(rowData[1].trim())
                    );
                });
         */

        windowedStream3.print();

        env.execute("Windows");
    }

    public static class ParseRow implements MapFunction<String, Tuple2<String, Integer>> {
        public Tuple2<String, Integer> map(String input) throws Exception {
            try {
                String[] rowData = input.split(",");
                return new Tuple2<>(
                        rowData[0].trim(),
                        Integer.parseInt(rowData[1].trim())
                );
            } catch (Exception e) {
                System.out.println("Exception in ParseRow");
            }
            return null;
        }
    }

}
