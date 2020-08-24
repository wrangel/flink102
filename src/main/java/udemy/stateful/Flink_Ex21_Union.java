package udemy.stateful;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import udemy.utils.StreamUtil;

/* Union appends multiple streams in one stream
       Input: Stream with space delimited numberss
       Result: 2 streams, one containing sum of the numbers, the other the product of the numbers,
        which then are combined by union operation
 */
public class Flink_Ex21_Union {

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
        DataStream<Tuple3<String, String, Double>> outStream = stream1.union(stream2);

        outStream.print();

        env.execute("Union");
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

}
