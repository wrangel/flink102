package udemy.stateful;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import udemy.utils.StreamUtil;

import java.util.HashMap;
import java.util.Map;

/* Stateless transformations apply only on one entity at a time
    - Examples: filter, map, flatMap
    - Make stateless transformations stateful w/ help of State objects: Raw state, Managed state
    - Input: Stream of words, e.g. a, b, c, print
    - Parameters: -host localhost --port 8081
 */
public class Flink_Ex17_ValueState {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> wordCountStream = StreamUtil.getDataStream(env, params)
                // Create tuple i.o.t. create KeyedStream afterwards
                .map(new WordToTuple())
                .keyBy(tuple -> tuple.f0)
                // Apply a STATEFUL flatMap operation, which is capable of maintaining state
                .flatMap(new collectTotalWordCount());

        wordCountStream.print();

        env.execute("Custom Source and Sink");
    }

    public static class WordToTuple implements MapFunction<String, Tuple2<Integer, String>> {
        public Tuple2<Integer, String> map(String input) throws Exception {
            return Tuple2.of(1, input.trim().toLowerCase());
        }
    }

    // Use RichFlatMapFunction in stead of regular FlatMapFunction
    public static class collectTotalWordCount extends RichFlatMapFunction<Tuple2<Integer, String>, String> {
        // Use value state whenever you have a single object to represent state
        // Store count of words in this object
        private transient ValueState<Map<String, Integer>> allWordCounts;

        public void flatMap(Tuple2<Integer, String> input, Collector<String> output) throws Exception {
            // Get the current value of the state
            Map<String, Integer> currentWordCounts = allWordCounts.value();
            if(input.f1.equals("print")){
                output.collect(currentWordCounts.toString());
                allWordCounts.clear();
            }
            // Update map in case the word is different from "print"
            if(!currentWordCounts.containsKey(input.f1)) {
                currentWordCounts.put(input.f1, 1);
            } else {
                Integer wordCount = currentWordCounts.get(input.f1);
                currentWordCounts.put(input.f1, 1 + wordCount);
            }
                    allWordCounts.update(currentWordCounts);
        }

        // Each FlatMap function has a method "open", which initializes the value state
        @Override
        public void open(Configuration config) {
            // Will be used later on to create value state object
            ValueStateDescriptor<Map<String, Integer>> derscriptor = new ValueStateDescriptor<>(
                    // State name
                    "allWordCounts",
                    // Type information of object which is held in the value state
                    TypeInformation.of(new TypeHint<Map<String, Integer>>() {}),
                    // Default value of the state, if nothing was set
                    new HashMap<String, Integer>()
            );
            // Get actual value statee object
            allWordCounts = getRuntimeContext().getState(derscriptor);
        }
    }

}
