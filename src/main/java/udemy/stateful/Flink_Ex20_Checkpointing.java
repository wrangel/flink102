package udemy.stateful;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import udemy.utils.StreamUtil;

/*    Ability to store state is important to enable fault tolerance for the application
        - Done by checkpointing, into specified persistent storage
        - After crash, recover state from checkpoint
        - This class is equivalent to Ex19, except for checkpointing and restart strategies
 */
public class Flink_Ex20_Checkpointing {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);
        // Enable checkpointing every 1000 ms
        env.enableCheckpointing(1000);
        /* Guarantee level: Set mode to "exactly once" (default): Every entity is processed by transformations
            exactly once
            at least once is useful for low-latency application, where waiting on exactly once paradime to be
            applied is not possible
         */
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // Ensure that there is no overhead to the application
        // 1) Avoid checkpointing w/o processing
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        // 2) Allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // Set serialization location. After crash, application will collect state from this location
        env.setStateBackend(new FsStateBackend("file:///~/sandbox/flinkCheckpoints"));
        // Define restart strategy. Default is fixed delay
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                2,
                2000
        ));

        DataStream<String> outStream = StreamUtil.getDataStream(env, params)
                // Create tuple i.o.t. create KeyedStream afterwards
                .map(new WordToTuple())
                .keyBy(tuple -> tuple.f0)
                // Apply a STATEFUL flatMap operation, which is capable of maintaining state
                .flatMap(new collectTotalWordCount());

        outStream.print();

        env.execute("Checkpointing");
    }

    // Creates a tuple of static key (1) and word (Workaround to get a Keyed Stream)
    public static class WordToTuple implements MapFunction<String, Tuple2<Integer, String>> {
        public Tuple2<Integer, String> map(String input) throws Exception {
            return Tuple2.of(1, input.trim().toLowerCase());
        }
    }

    // Use RichFlatMapFunction in stead of regular FlatMapFunction
    public static class collectTotalWordCount extends RichFlatMapFunction<Tuple2<Integer, String>, String> {
        // Use value state whenever you have a single object to represent state
        // Store distinct list of words in this object
        private transient ReducingState<Integer> totalCountState;

        // Update and clear state object
        public void flatMap(Tuple2<Integer, String> input, Collector<String> output) throws Exception {
            if (input.f1.equals("print")) {
                // Print out current state
                output.collect(totalCountState.get().toString());
                totalCountState.clear();
            }
            // Run through in case word is sth other than print
            else
                totalCountState.add(1);
        }

        // Each FlatMap function has a method "open", which initializes the state object
        @Override
        public void open(Configuration config) {
            // Will be used later on to create value state object
            ReducingStateDescriptor<Integer> descriptor = new ReducingStateDescriptor<>(
                    // State name
                    "totalCount",
                    // Reduce function
                    Integer::sum,
                    // Type information of object which is held in the value state
                    Integer.class
            );
            // Get actual value state object (initialize)
            totalCountState = getRuntimeContext().getReducingState(descriptor);
        }
    }

}
