package udemy.stateful;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import udemy.transformations.StreamKeySelectors.StreamKeySelector1;
import udemy.utils.StreamUtil;

/* Join two keyed streams based on their keys
 */
public class Flink_Ex22_JoiningStreams {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

        // Sum of numbers stream
        DataStream<Tuple3<String, String, Double>> stream1 = dataStream.map(new SumNumbers());

        // Product of numbers stream
        DataStream<Tuple3<String, String, Double>> stream2 = dataStream.map(new MultiplyNumbers());

        // Union stream
        DataStream<Tuple3<String, Double, Double>> joinedStream = stream1.join(stream2)
                // Specify keys in both streams
                .where(new StreamKeySelector1()).equalTo(new StreamKeySelector1())
                // Define window within which matching keys are to be found in entities
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                // How to combine each pair of matching tuples
                .apply(new SumProductJoinFunction());

        joinedStream.print();

        env.execute("Joining Streams");
    }

    public static class SumNumbers implements MapFunction<String, Tuple3<String, String, Double>> {
        public Tuple3<String, String, Double> map(String input) throws Exception {
            String[] nums = input.split(" ");
            double sum = 0.0;
            for (String num : nums) {
                sum = sum + Double.parseDouble(num);
            }
            return Tuple3.of(input, "Sum", sum);
        }
    }

    public static class MultiplyNumbers implements MapFunction<String, Tuple3<String, String, Double>> {
        public Tuple3<String, String, Double> map(String input) throws Exception {
            String[] nums = input.split(" ");
            double product = 1.0;
            for (String num : nums) {
                product = product * Double.parseDouble(num);
            }
            return Tuple3.of(input, "Product", product);
        }
    }

    // Three template parameters: Data type from stream 1, same for stream 2, Output data type
    private static class SumProductJoinFunction implements JoinFunction<Tuple3
            <String, String, Double>,
            Tuple3<String, String, Double>,
            Tuple3<String, Double, Double>
            > {
        public Tuple3<String, Double, Double> join(
                Tuple3<String, String, Double> first,
                Tuple3<String, String, Double> second
        ) {
            // Returns key, sum of numbers, product of numbers
            return Tuple3.of(first.f0, first.f2, second.f2);
        }
    }

}
