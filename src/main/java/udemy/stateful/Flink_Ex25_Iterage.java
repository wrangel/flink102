package udemy.stateful;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/* Iterate: loop over a stream over and over again
    - After applying iterate operator to a stream, the stram accepts output back to itself
    - no input parameters needed for this stream
 */
public class Flink_Ex25_Iterage {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Integer> stream = env.fromElements(32, 17, 30, 27);

        IterativeStream<Tuple2<Integer, Integer>> iterativeStream = stream
                .map(new AddIterCount())
                .iterate();

        DataStream<Tuple2<Integer, Integer>> checkMultiple4Stream = iterativeStream.map(new CheckMultiple4());

        // Filter is used to adds elements not equal to 4 to the feedback stream
        DataStream<Tuple2<Integer, Integer>> feedback = checkMultiple4Stream.filter((value) -> value.f0 % 4 != 0);

        // Close the loop between iterative stream and feedback stream
        iterativeStream.closeWith(feedback);

        DataStream<Tuple2<Integer, Integer>> outputStream = checkMultiple4Stream.filter((value) -> value.f0 % 4 == 0);

        outputStream.print();

        env.execute("Iterate");
    }

    // Returns input, as well as number of iterations the input event has gone through
    public static class AddIterCount implements MapFunction<Integer, Tuple2<Integer, Integer>> {
        public Tuple2<Integer, Integer> map(Integer input) throws Exception {
            return Tuple2.of(input, 0);
        }
    }

    public static class CheckMultiple4 implements MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
        public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> input) throws Exception {
            Tuple2<Integer, Integer> output;
            if (input.f0 % 4 == 0)
                // Send out input to output in case input is a multiple of 4
                output = input;
            else
                // Exif if input is not a multiple of 4
                output = Tuple2.of(input.f0 - 1, input.f1 + 1);
            return output;
        }
    }

}
