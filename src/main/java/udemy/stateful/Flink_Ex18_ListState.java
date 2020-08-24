package udemy.stateful;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import udemy.utils.StreamUtil;

/* Another type of state object
    - Input: Stream of words, e.g. a, b, c, print. Same use case as in Ex17
    - State in this case: distinct list of words encountered so far
    - Whenever list is needed to be maintained, it is done via list state object
    - Parameters: -host localhost --port 8081
 */
public class Flink_Ex18_ListState {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> outStream = StreamUtil.getDataStream(env, params)
                // Create tuple i.o.t. create KeyedStream afterwards
                .map(new WordToTuple())
                .keyBy(tuple -> tuple.f0)
                // Apply a STATEFUL flatMap operation, which is capable of maintaining state
                .flatMap(new collectDistinctWords());

        outStream.print();

        env.execute("List state");
    }

    // Creates a tuple of static key (1) and word (Workaround to get a Keyed Stream)
    public static class WordToTuple implements MapFunction<String, Tuple2<Integer, String>> {
        public Tuple2<Integer, String> map(String input) throws Exception {
            return Tuple2.of(1, input.trim().toLowerCase());
        }
    }

    // Use RichFlatMapFunction in stead of regular FlatMapFunction
    public static class collectDistinctWords extends RichFlatMapFunction<Tuple2<Integer, String>, String> {
        // Use value state whenever you have a single object to represent state
        // Store distinct list of words in this object
        private transient ListState<String> distinctWordList;

        // Update and clear state object
        public void flatMap(Tuple2<Integer, String> input, Collector<String> output) throws Exception {
            // Get the current value of the state
            Iterable<String> currentWordList = distinctWordList.get();
            boolean oldWord = false;
            if (input.f1.equals("print")) {
                // Print out current state
                output.collect(distinctWordList.toString());
                distinctWordList.clear();
            }
            // Run through in case word is sth other than print
            else {
                for (String word : currentWordList) {
                    if (input.f1.equals(word)) {
                        oldWord = true;
                        break;
                    }
                }
                if (!oldWord)
                    distinctWordList.add(input.f1);
            }
        }

        // Each FlatMap function has a method "open", which initializes the state object
        @Override
        public void open(Configuration config) {
            // Will be used later on to create value state object
            ListStateDescriptor<String> descriptor = new ListStateDescriptor<>(
                    // State name
                    "wordList",
                    // Type information of object which is held in the value state
                    String.class
            );
            // Get actual value state object (initialize)
            distinctWordList = getRuntimeContext().getListState(descriptor);
        }
    }

}
